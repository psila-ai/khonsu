use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{Schema, Field, DataType};

use khonsu::{Khonsu, TransactionIsolation, conflict::resolution::ConflictResolution, errors::Error};

mod mock_storage;

// Configure tests to run single-threaded
#[cfg(test)]
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

    #[test]
    fn test_serializable_wrw_conflict() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage.clone(), TransactionIsolation::Serializable, ConflictResolution::Fail);

        // Define a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let id_array = Arc::new(StringArray::from(vec!["key1"]));
        let value_array = Arc::new(Int64Array::from(vec![100]));
        let initial_record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn.write("key1".to_string(), initial_record_batch.clone()).unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is in storage
        assert!(storage.get("key1").is_some());

        // Scenario: W-R-W conflict
        // Tx1 writes "key1"
        // Tx2 reads "key1"
        // Tx1 reads "key1" (after Tx2 read)
        // Tx1 commits (should fail due to W-R-W cycle)

        // Tx1 starts and writes
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let id_array_tx1 = Arc::new(StringArray::from(vec!["key1"]));
        let value_array_tx1 = Arc::new(Int64Array::from(vec![200]));
        let record_batch_tx1 = RecordBatch::try_new(Arc::clone(&schema), vec![id_array_tx1, value_array_tx1]).unwrap();
        txn1.write("key1".to_string(), record_batch_tx1.clone()).unwrap();
        println!("Tx1 ({}) wrote key1", txn1_id);


        // Tx2 starts and reads
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();
        let read_batch_tx2 = txn2.read(&"key1".to_string()).unwrap().unwrap();
        println!("Tx2 ({}) read key1", txn2_id);
        // Assert Tx2 read the initial data
        assert_eq!(&*read_batch_tx2, &initial_record_batch);


        // Tx1 reads (after Tx2 read)
        let read_batch_tx1 = txn1.read(&"key1".to_string()).unwrap().unwrap();
        println!("Tx1 ({}) read key1", txn1_id);
        // Assert Tx1 reads its own write (most likely, depending on read implementation)
        // Or it might read the initial data if its write is not yet visible to itself.
        // With the current implementation, it should read its own write from the write_set.
        assert_eq!(*read_batch_tx1, record_batch_tx1);


        // Attempt to commit Tx1 (should fail)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);

        // Verify Tx1 commit failed with TransactionConflict
        assert!(commit_result_tx1.is_err());
        match commit_result_tx1.unwrap_err() {
            Error::TransactionConflict => {
                println!("Tx1 correctly failed with TransactionConflict");
            }
            e => panic!("Tx1 failed with unexpected error: {:?}", e),
        }

        // Commit Tx2 (should succeed as Tx1 aborted)
        println!("Attempting to commit Tx2 ({})", txn2_id);
        let commit_result_tx2 = txn2.commit();
        println!("Tx2 commit result: {:?}", commit_result_tx2);

        // Verify Tx2 commit succeeded
        assert!(commit_result_tx2.is_ok());

        // Verify the data in storage is still the initial data (Tx1 aborted, Tx2 only read)
        let final_stored_batch = storage.get("key1").unwrap();
        assert_eq!(final_stored_batch, initial_record_batch);
    }

    #[test]
    fn test_serializable_rw_conflict() {
        let storage = Arc::new(mock_storage::MockStorage::new());
        let khonsu = Khonsu::new(storage.clone(), TransactionIsolation::Serializable, ConflictResolution::Fail);

        // Define a simple schema and record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let id_array = Arc::new(StringArray::from(vec!["key1"]));
        let value_array = Arc::new(Int64Array::from(vec![100]));
        let initial_record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![id_array, value_array]).unwrap();

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn.write("key1".to_string(), initial_record_batch.clone()).unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is in storage
        assert!(storage.get("key1").is_some());

        // Scenario: R-W conflict
        // Tx1 reads "key1"
        // Tx2 writes "key1"
        // Tx1 commits (should succeed if Tx2 hasn't committed yet)
        // Tx2 commits (should fail if Tx1 committed)

        // Tx1 starts and reads
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let read_batch_tx1 = txn1.read(&"key1".to_string()).unwrap().unwrap();
        println!("Tx1 ({}) read key1", txn1_id);
        // Assert Tx1 read the initial data
        assert_eq!(&*read_batch_tx1, &initial_record_batch);

        // Tx2 starts and writes
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();
        let id_array_tx2 = Arc::new(StringArray::from(vec!["key1"]));
        let value_array_tx2 = Arc::new(Int64Array::from(vec![300]));
        let record_batch_tx2 = RecordBatch::try_new(Arc::clone(&schema), vec![id_array_tx2, value_array_tx2]).unwrap();
        txn2.write("key1".to_string(), record_batch_tx2.clone()).unwrap();
        println!("Tx2 ({}) wrote key1", txn2_id);

        // Attempt to commit Tx1 (should succeed)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);

        // Verify Tx1 commit succeeded
        assert!(commit_result_tx1.is_ok());

        // Verify the data in storage is still the initial data (Tx1 only read)
        let stored_batch_after_tx1 = storage.get("key1").unwrap();
        assert_eq!(stored_batch_after_tx1, initial_record_batch);


        // Attempt to commit Tx2 (should fail due to R-W conflict with committed Tx1)
        println!("Attempting to commit Tx2 ({})", txn2_id);
        let commit_result_tx2 = txn2.commit();
        println!("Tx2 commit result: {:?}", commit_result_tx2);

        // Verify Tx2 commit failed with TransactionConflict
        assert!(commit_result_tx2.is_err());
        match commit_result_tx2.unwrap_err() {
            Error::TransactionConflict => {
                println!("Tx2 correctly failed with TransactionConflict");
            }
            e => panic!("Tx2 failed with unexpected error: {:?}", e),
        }

        // Verify the data in storage is still the initial data (Tx2 aborted)
        let final_stored_batch = storage.get("key1").unwrap();
        assert_eq!(final_stored_batch, initial_record_batch);
    }
}
