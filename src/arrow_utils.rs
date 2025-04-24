use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, StringArray, Array, PrimitiveArray, Int64Array, Float64Array, BooleanArray};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use arrow::buffer::Buffer;
use arrow::array::builder::{StringBuilder, Int64Builder, Float64Builder, BooleanBuilder}; // Use StringBuilder and add new builders

use crate::errors::{Result, Error}; // Assuming errors module is at crate root

/// Merges changes from a write set (insertions, updates, deletions) into a target RecordBatch.
///
/// This function is comprehensive and handles all types of changes
/// from a transaction's write set.
pub fn merge_record_batches(
    target: &RecordBatch,
    write_set: &HashMap<String, Option<RecordBatch>>,
    key_column_name: &str,
) -> Result<RecordBatch> {
    // 1. Separate changes into insertions, updates, and deletions.
    let mut insertions: Vec<(String, RecordBatch)> = Vec::new();
    let mut updates: Vec<(String, RecordBatch)> = Vec::new();
    let mut deletions: HashSet<String> = HashSet::new(); // Use HashSet for efficient lookups

    // Get the key column index and array from the target for efficient lookup
    let target_key_column_index = target.schema().column_with_name(key_column_name)
        .ok_or_else(|| Error::ArrowError(format!("Key column '{}' not found in target RecordBatch", key_column_name)))?.0;
    let target_key_array = target.column(target_key_column_index).as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| Error::ArrowError(format!("Key column '{}' is not a StringArray in target", key_column_name)))?;

    // Build a set of existing keys in the target for efficient lookup
    let existing_target_keys: HashSet<&str> = target_key_array.iter()
        .filter_map(|key| key)
        .collect();

    for (key, change) in write_set {
        match change {
            Some(record_batch) => {
                // Determine if this is an insert or update by checking if the key exists in the target.
                if existing_target_keys.contains(key.as_str()) {
                    updates.push((key.clone(), record_batch.clone()));
                } else {
                    insertions.push((key.clone(), record_batch.clone()));
                }
            }
            None => {
                deletions.insert(key.clone());
            }
        }
    }

    // 2. Apply updates to the target RecordBatch and filter out deletions.
    // We will build the new arrays directly, skipping deleted rows and applying updates.
    let target_schema = target.schema();
    let mut new_arrays: Vec<Box<dyn arrow::array::ArrayBuilder>> = target_schema.fields().iter()
        .map(|field| create_builder(field.data_type(), target.num_rows())) // Create appropriate builders
        .collect();

    let mut target_row_map: HashMap<&str, usize> = HashMap::new();
     target_key_array.iter().enumerate().for_each(|(i, key)| {
        if let Some(k) = key {
            target_row_map.insert(k, i);
        }
    });


    for i in 0..target.num_rows() {
        let key = target_key_array.value(i);

        // Check if this row is marked for deletion
        if deletions.contains(key) {
            continue; // Skip deleted rows
        }

        // Check if this row is updated
        let mut updated = false;
        if let Some((_, update_batch)) = updates.iter().find(|(update_key, _)| update_key == key) {
             // Apply the update for this row across all columns.
            for col_index in 0..target_schema.fields().len() {
                let target_field = &target_schema.field(col_index);
                let update_array = update_batch.column(col_index);

                // Append the updated value to the new array builder
                append_value(&mut new_arrays[col_index], update_array, 0, target_field.data_type())?; // Assuming update batch has only one row for this key
            }
            updated = true;
        }

        // If not updated and not deleted, append the original value
        if !updated {
            for col_index in 0..target_schema.fields().len() {
                let target_array = target.column(col_index);
                let target_field = &target_schema.field(col_index);
                append_value(&mut new_arrays[col_index], target_array, i, target_field.data_type())?;
            }
        }
    }

    // 3. Append new rows (insertions).
    for (_, insertion_batch) in insertions {
        // Ensure schema compatibility (basic check)
        if insertion_batch.schema() != target_schema {
            return Err(Error::ArrowError("Schema mismatch between target and insertion RecordBatch".to_string()));
        }

        for i in 0..insertion_batch.num_rows() {
            for col_index in 0..target_schema.fields().len() {
                let insertion_array = insertion_batch.column(col_index);
                let target_field = &target_schema.field(col_index);
                 append_value(&mut new_arrays[col_index], insertion_array, i, target_field.data_type())?;
            }
        }
    }

    // 4. Finalize the new arrays and create the result RecordBatch.
    let final_arrays: Vec<ArrayRef> = new_arrays.into_iter()
        .map(|mut builder| builder.finish())
        .collect();

    let result_record_batch = RecordBatch::try_new(target_schema.clone(), final_arrays)
        .map_err(|e| Error::ArrowError(format!("Failed to create result RecordBatch: {}", e)))?;

    Ok(result_record_batch)
}

/// Helper function to create an appropriate ArrayBuilder based on DataType.
fn create_builder(data_type: &DataType, capacity: usize) -> Box<dyn arrow::array::ArrayBuilder> {
    match data_type {
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()), // Add Float64Builder
        DataType::Boolean => Box::new(BooleanBuilder::new()), // Add BooleanBuilder
        // TODO: Add support for other Arrow Data Types
        _ => panic!("Unsupported Arrow Data Type for builder: {:?}", data_type),
    }
}

/// Helper function to append a value from a source array to a builder.
fn append_value(
    builder: &mut Box<dyn arrow::array::ArrayBuilder>,
    source_array: &ArrayRef,
    source_row_index: usize,
    data_type: &DataType,
) -> Result<()> {
    match data_type {
        DataType::Utf8 => {
            let builder = builder.as_any_mut().downcast_mut::<StringBuilder>() // Use StringBuilder
                .ok_or_else(|| Error::ArrowError("Failed to downcast builder to StringBuilder".to_string()))?;
            let source_array = source_array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast source array to StringArray".to_string()))?;
            
            let value = source_array.value(source_row_index);
            builder.append_value(value);
        }
        DataType::Int64 => {
            let builder = builder.as_any_mut().downcast_mut::<Int64Builder>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast builder to Int64Builder".to_string()))?;
            let source_array = source_array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast source array to Int64Array".to_string()))?;
             
            let value = source_array.value(source_row_index);
            builder.append_value(value);
        }
        DataType::Float64 => { // Add Float64 handling
            let builder = builder.as_any_mut().downcast_mut::<Float64Builder>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast builder to Float64Builder".to_string()))?;
            let source_array = source_array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast source array to Float64Array".to_string()))?;
            
            let value = source_array.value(source_row_index);
            builder.append_value(value);
        }
        DataType::Boolean => { // Add Boolean handling
            let builder = builder.as_any_mut().downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast builder to BooleanBuilder".to_string()))?;
            let source_array = source_array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::ArrowError("Failed to downcast source array to BooleanArray".to_string()))?;
            
            let value = source_array.value(source_row_index);
            builder.append_value(value);
        }
        // TODO: Add support for other Arrow Data Types
        _ => return Err(Error::ArrowError(format!("Unsupported Arrow Data Type for appending: {:?}", data_type))),
    }
    Ok(())
}