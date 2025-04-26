#![allow(unused)]
//! Common utilities and mock implementations for Khonsu integration tests.
//!
//! This module provides shared helper functions and a mock storage implementation
//! to facilitate writing integration tests for the Khonsu STM library.

use ahash::AHashMap as HashMap;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use khonsu::{
    conflict::resolution::ConflictResolution,
    errors::Result,
    storage::{Storage, StorageMutation},
    Khonsu, TransactionIsolation,
};
use std::sync::{Arc, Mutex};

// --- MockStorage ---

/// A mock implementation of the `Storage` trait for testing purposes.
/// Stores records in an in-memory HashMap protected by a Mutex.
#[derive(Debug)] // Added Debug derive
pub struct MockStorage {
    data: Mutex<HashMap<String, RecordBatch>>,
}

impl MockStorage {
    /// Creates a new `MockStorage` instance.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieves a record from the mock storage.
    pub fn get(&self, key: &str) -> Option<RecordBatch> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

impl Storage for MockStorage {
    /// Applies the given mutations to the in-memory mock storage.
    ///
    /// This mock implementation simulates atomic application by acquiring a
    /// mutex lock for the duration of the operation. In a real storage
    /// implementation, this would involve durable, atomic writes.
    fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        for mutation in mutations {
            match mutation {
                StorageMutation::Insert(key, record_batch) => {
                    data.insert(key, record_batch);
                }
                StorageMutation::Delete(key) => {
                    data.remove(&key);
                }
            }
        }
        Ok(())
    }
}

// --- Helper Functions ---

/// Helper function to create a Khonsu instance with MockStorage and specified isolation.
///
/// This function simplifies setting up a Khonsu instance for tests by providing
/// a default mock storage and allowing specification of the transaction isolation level.
pub fn setup_khonsu(isolation: TransactionIsolation) -> Arc<Khonsu> {
    let storage = Arc::new(MockStorage::new()); // Wrap MockStorage in Arc for dyn Storage
    Arc::new(Khonsu::new(
        storage,
        isolation,
        ConflictResolution::Fail, // Default conflict resolution
    ))
}

/// Helper function to create a simple schema for test RecordBatches.
pub fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

/// Helper function to create a RecordBatch with a predefined schema.
///
/// # Arguments
///
/// * `ids` - A vector of i64 values for the "id" column.
/// * `values` - A vector of optional string slices for the "value" column.
///
/// # Returns
///
/// A new `RecordBatch` with the test schema and provided data.
///
/// # Panics
///
/// Panics if the data cannot be converted into a valid RecordBatch with the schema.
pub fn create_record_batch(ids: Vec<i64>, values: Vec<Option<&str>>) -> RecordBatch {
    let schema = create_test_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}
