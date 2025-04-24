use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};

use khonsu::{Khonsu, TransactionIsolation, conflict::resolution::ConflictResolution};

mod mock_storage;

// Configure tests to run single-threaded
#[cfg(test)]
#[cfg(not(loom))] // Exclude when using Loom for concurrency testing
mod single_threaded_tests {
    use super::*;

    #[test]
    fn test_basic_khonsu_creation() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage, TransactionIsolation::ReadCommitted, ConflictResolution::Fail);

        // Assert that the Khonsu instance is created and transaction IDs are incrementing
        assert_eq!(khonsu.start_transaction().id(), 0);
        assert_eq!(khonsu.start_transaction().id(), 1);
    }

    #[test]
    fn test_basic_read_write_commit() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage.clone(), TransactionIsolation::ReadCommitted, ConflictResolution::Fail);

        // Define a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let id_array = Arc::new(StringArray::from(vec!["key1"]));
        let value_array = Arc::new(Int64Array::from(vec![100]));
        let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

        // Start a transaction, write data, and commit
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch.clone()).unwrap();
        txn.commit().unwrap();

        // Verify the data is in the mock storage
        let stored_batch = storage.get("key1").unwrap();
        assert_eq!(stored_batch.num_rows(), 1);
        let stored_id_array = stored_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let stored_value_array = stored_batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(stored_id_array.value(0), "key1");
        assert_eq!(stored_value_array.value(0), 100);

        // Start another transaction and read the data
        let mut txn2 = khonsu.start_transaction();
        let read_batch = txn2.read(&"key1".to_string()).unwrap().unwrap();
        assert_eq!(*read_batch, record_batch); // Verify read data matches original
    }

    #[test]
    fn test_basic_delete_commit() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage.clone(), TransactionIsolation::ReadCommitted, ConflictResolution::Fail);

        // Define a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let id_array = Arc::new(StringArray::from(vec!["key1"]));
        let value_array = Arc::new(Int64Array::from(vec![100]));
        let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

        // Write and commit initial data
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch).unwrap();
        txn.commit().unwrap();

        // Verify data is in storage
        assert!(storage.get("key1").is_some());

        // Start a new transaction, delete data, and commit
        let mut txn2 = khonsu.start_transaction();
        txn2.delete("key1").unwrap();
        txn2.commit().unwrap();

        // Verify data is deleted from storage
        assert!(storage.get("key1").is_none());

        // Start another transaction and try to read the deleted data
        let mut txn3 = khonsu.start_transaction();
        let read_batch = txn3.read(&"key1".to_string()).unwrap();
        assert!(read_batch.is_none()); // Verify data is not found
    }

    #[test]
    fn test_basic_rollback() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage.clone(), TransactionIsolation::ReadCommitted, ConflictResolution::Fail);

        // Define a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let id_array = Arc::new(StringArray::from(vec!["key1"]));
        let value_array = Arc::new(Int64Array::from(vec![100]));
        let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

        // Start a transaction, write data, and rollback
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch).unwrap();
        txn.rollback(); // Rollback the transaction

        // Verify the data is NOT in the mock storage
        assert!(storage.get("key1").is_none());

        // Start another transaction and try to read the data
        let mut txn2 = khonsu.start_transaction();
        let read_batch = txn2.read(&"key1".to_string()).unwrap();
        assert!(read_batch.is_none()); // Verify data is not found
    }

    // TODO: Add more test cases for different isolation levels and conflict resolution strategies.
    // TODO: Add test cases for concurrent transactions.
}
