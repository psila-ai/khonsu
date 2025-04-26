// Declare the common module
mod common;

/// Basic transaction tests for the Khonsu Software Transactional Memory library.
///
/// This module contains fundamental tests for transaction creation, basic read,
/// write, delete, commit, and rollback operations, as well as initial tests
/// for Serializable isolation conflicts (W-R-W, R-W, W-W) and dependency tracking cleanup.
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// Use khonsu:: prefix for library items
use khonsu::{errors::KhonsuError, TransactionIsolation};
// Use common:: prefix for shared test utilities

// Configure tests to run single-threaded
#[cfg(test)]
mod single_threaded_tests {
    // Import common items needed within the test module
    use super::*; // Brings in Arc, arrow types, khonsu types, etc.
    use crate::common::setup_khonsu; // Use crate::common here

    // Helper to create schema, specific to this file's tests if needed, or use common one
    fn create_basic_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false), // Using Utf8 key here
            Field::new("value", DataType::Int64, false),
        ]))
    }

    // Helper to create batch, specific to this file's tests if needed, or use common one
    fn create_basic_record_batch(
        schema: Arc<Schema>,
        ids: Vec<&str>,
        values: Vec<i64>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_basic_khonsu_creation() {
        // Use setup_khonsu from common module
        let khonsu = setup_khonsu(TransactionIsolation::ReadCommitted);

        // Assert that the Khonsu instance is created and transaction IDs are incrementing
        assert_eq!(khonsu.start_transaction().id(), 0);
        assert_eq!(khonsu.start_transaction().id(), 1);
    }

    #[test]
    fn test_basic_read_write_commit() {
        let khonsu = setup_khonsu(TransactionIsolation::ReadCommitted);
        // Get underlying MockStorage for direct verification if needed
        // Note: This requires adding a way to get the storage back from Khonsu or setup_khonsu,
        // or modifying MockStorage to allow inspection without direct access via Khonsu.
        // For now, we rely on reading back through another transaction.

        let schema = create_basic_schema();
        let record_batch = create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Start a transaction, write data, and commit
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch.clone()).unwrap();
        txn.commit().unwrap();

        // Verify the data by reading it back in a new transaction
        let mut verify_txn = khonsu.start_transaction();
        let stored_batch = verify_txn.read(&"key1".to_string()).unwrap().unwrap();
        // assert_eq!(stored_batch.num_rows(), 1); // Redundant if batches equal
        let stored_id_array = stored_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let stored_value_array = stored_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(stored_id_array.value(0), "key1");
        assert_eq!(stored_value_array.value(0), 100);

        // Start another transaction and read the data
        let mut txn2 = khonsu.start_transaction();
        let read_batch = txn2.read(&"key1".to_string()).unwrap().unwrap();
        assert_eq!(*read_batch, record_batch); // Verify read data matches original
    }

    #[test]
    fn test_basic_delete_commit() {
        let khonsu = setup_khonsu(TransactionIsolation::ReadCommitted);

        let schema = create_basic_schema();
        let record_batch = create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Write and commit initial data
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch).unwrap();
        txn.commit().unwrap();

        // Verify data is present by reading
        let mut verify_txn_before = khonsu.start_transaction();
        assert!(verify_txn_before
            .read(&"key1".to_string())
            .unwrap()
            .is_some());
        verify_txn_before.rollback(); // Don't need to commit read

        // Start a new transaction, delete data, and commit
        let mut txn2 = khonsu.start_transaction();
        txn2.delete("key1").unwrap();
        txn2.commit().unwrap();

        // Verify data is deleted by trying to read
        // Start another transaction and try to read the deleted data
        let mut txn3 = khonsu.start_transaction();
        let read_batch = txn3.read(&"key1".to_string()).unwrap();
        assert!(read_batch.is_none()); // Verify data is not found
    }

    #[test]
    fn test_basic_rollback() {
        let khonsu = setup_khonsu(TransactionIsolation::ReadCommitted);

        let schema = create_basic_schema();
        let record_batch = create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Start a transaction, write data, and rollback
        let mut txn = khonsu.start_transaction();
        txn.write("key1".to_string(), record_batch).unwrap();
        txn.rollback(); // Rollback the transaction

        // Verify the data is NOT present by trying to read
        // Start another transaction and try to read the data
        let mut txn2 = khonsu.start_transaction();
        let read_batch = txn2.read(&"key1".to_string()).unwrap();
        assert!(read_batch.is_none()); // Verify data is not found
    }

    // TODO: Add more test cases for different isolation levels and conflict resolution strategies.
    // TODO: Add test cases for concurrent transactions.

    #[test]
    fn test_serializable_wrw_conflict() {
        let khonsu = setup_khonsu(TransactionIsolation::Serializable);

        let schema = create_basic_schema();
        let initial_record_batch =
            create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn
            .write("key1".to_string(), initial_record_batch.clone())
            .unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is present by reading
        let mut verify_txn_initial = khonsu.start_transaction();
        assert!(verify_txn_initial
            .read(&"key1".to_string())
            .unwrap()
            .is_some());
        verify_txn_initial.rollback();

        // Scenario: W-R-W conflict (Adjusted for SSI)
        // Tx1 writes "key1"
        // Tx2 reads "key1"
        // Tx1 reads "key1" (after Tx2 read)
        // Tx1 commits (should fail due to W-R-W cycle)

        // Tx1 starts and writes
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let record_batch_tx1 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![200]);
        txn1.write("key1".to_string(), record_batch_tx1.clone())
            .unwrap();
        println!("Tx1 ({}) wrote key1", txn1_id);

        // Tx2 starts and reads
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();
        let read_batch_tx2 = txn2
            .read(&"key1".to_string())
            .expect("Error reading data in Tx2")
            .unwrap();
        println!("Tx2 ({}) read key1", txn2_id);
        // Assert Tx2 read the initial data
        assert_eq!(&*read_batch_tx2, &initial_record_batch);

        // Tx1 reads (after Tx2 read)
        let read_batch_tx1 = txn1
            .read(&"key1".to_string())
            .expect("Error reading data in Tx1")
            .unwrap();
        println!("Tx1 ({}) read key1", txn1_id);
        // Assert Tx1 reads its own write (most likely, depending on read implementation)
        // Or it might read the initial data if its write is not yet visible to itself.
        // With the current implementation, it should read its own write from the write_set.
        assert_eq!(*read_batch_tx1, record_batch_tx1);

        // Attempt to commit Tx1 (should succeed under SSI as no dangerous RW structure exists yet)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);

        // Verify Tx1 commit succeeded
        assert!(commit_result_tx1.is_ok());

        // Commit Tx2 (should succeed as Tx1 committed successfully)
        println!("Attempting to commit Tx2 ({})", txn2_id);
        let commit_result_tx2 = txn2.commit();
        println!("Tx2 commit result: {:?}", commit_result_tx2);

        // Verify Tx2 commit failed (due to backward validation conflict with committed Tx1)
        assert!(commit_result_tx2.is_err());
        if let Err(KhonsuError::TransactionConflict) = commit_result_tx2 {
            println!("Tx2 correctly failed with TransactionConflict (SSI Backward Validation)");
        } else {
            panic!("Tx2 failed with unexpected result: {:?}", commit_result_tx2);
        }

        // Verify the data in storage is now Tx1's data (Tx1 committed, Tx2 aborted)
        let mut verify_txn_final = khonsu.start_transaction();
        let final_stored_batch = verify_txn_final.read(&"key1".to_string()).unwrap().unwrap();
        // Tx1 wrote value 200
        let record_batch_tx1_final =
            create_basic_record_batch(schema.clone(), vec!["key1"], vec![200]);
        assert_eq!(*final_stored_batch, record_batch_tx1_final);
        verify_txn_final.rollback();
    }

    #[test]
    fn test_serializable_rw_conflict() {
        let khonsu = setup_khonsu(TransactionIsolation::Serializable);

        let schema = create_basic_schema();
        let initial_record_batch =
            create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn
            .write("key1".to_string(), initial_record_batch.clone())
            .unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is present by reading
        let mut verify_txn_initial = khonsu.start_transaction();
        assert!(verify_txn_initial
            .read(&"key1".to_string())
            .unwrap()
            .is_some());
        verify_txn_initial.rollback();

        // Scenario: R-W conflict (Adjusted for SSI)
        // Tx1 reads "key1"
        // Tx2 writes "key1"
        // Tx1 commits (should succeed if Tx2 hasn't committed yet)
        // Tx2 commits (should fail if Tx1 committed)

        // Tx1 starts and reads
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let read_batch_tx1 = txn1
            .read(&"key1".to_string())
            .expect("Error reading data in Tx1")
            .unwrap();
        println!("Tx1 ({}) read key1", txn1_id);
        // Assert Tx1 read the initial data
        assert_eq!(&*read_batch_tx1, &initial_record_batch);

        // Tx2 starts and writes
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();
        let record_batch_tx2 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![300]);
        txn2.write("key1".to_string(), record_batch_tx2.clone())
            .unwrap();
        println!("Tx2 ({}) wrote key1", txn2_id);

        // Attempt to commit Tx1 (should succeed)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);

        // Verify Tx1 commit succeeded
        assert!(commit_result_tx1.is_ok());

        // Verify the data in storage is still the initial data (Tx1 only read)
        let mut verify_txn_after_tx1 = khonsu.start_transaction();
        let stored_batch_after_tx1 = verify_txn_after_tx1
            .read(&"key1".to_string())
            .unwrap()
            .unwrap();
        assert_eq!(*stored_batch_after_tx1, initial_record_batch);
        verify_txn_after_tx1.rollback();

        // Attempt to commit Tx2 (should succeed under SSI as Tx1 only read)
        println!("Attempting to commit Tx2 ({})", txn2_id);
        let commit_result_tx2 = txn2.commit();
        println!("Tx2 commit result: {:?}", commit_result_tx2);

        // Verify Tx2 commit succeeded (SSI allows this as Tx1 only read, no dangerous structure)
        assert!(commit_result_tx2.is_ok());

        // Verify the data in storage is now Tx2's data
        let mut verify_txn_final = khonsu.start_transaction();
        let final_stored_batch = verify_txn_final.read(&"key1".to_string()).unwrap().unwrap();
        assert_eq!(*final_stored_batch, record_batch_tx2); // Tx2 wrote value 300
        verify_txn_final.rollback();
    }

    #[test]
    fn test_serializable_ww_conflict() {
        let khonsu = setup_khonsu(TransactionIsolation::Serializable);

        let schema = create_basic_schema();
        let initial_record_batch =
            create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn
            .write("key1".to_string(), initial_record_batch.clone())
            .unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is present by reading
        let mut verify_txn_initial = khonsu.start_transaction();
        assert!(verify_txn_initial
            .read(&"key1".to_string())
            .unwrap()
            .is_some());
        verify_txn_initial.rollback();

        // Scenario: W-W conflict
        // Tx1 writes "key1"
        // Tx2 writes "key1"
        // Tx1 commits (should succeed)
        // Tx2 commits (should fail due to W-W conflict with committed Tx1)

        // Tx1 starts and writes
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let record_batch_tx1 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![200]);
        txn1.write("key1".to_string(), record_batch_tx1.clone())
            .unwrap();
        println!("Tx1 ({}) wrote key1", txn1_id);

        // Tx2 starts and writes
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();
        let record_batch_tx2 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![300]);
        txn2.write("key1".to_string(), record_batch_tx2.clone())
            .unwrap();
        println!("Tx2 ({}) wrote key1", txn2_id);

        // Attempt to commit Tx1 (should succeed)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);

        // Verify Tx1 commit succeeded
        assert!(commit_result_tx1.is_ok());

        // Verify the data in storage is now Tx1's data
        let mut verify_txn_after_tx1 = khonsu.start_transaction();
        let stored_batch_after_tx1 = verify_txn_after_tx1
            .read(&"key1".to_string())
            .unwrap()
            .unwrap();
        assert_eq!(*stored_batch_after_tx1, record_batch_tx1);
        verify_txn_after_tx1.rollback();

        // Attempt to commit Tx2 (should fail due to W-W conflict with committed Tx1)
        println!("Attempting to commit Tx2 ({})", txn2_id);
        let commit_result_tx2 = txn2.commit();
        println!("Tx2 commit result: {:?}", commit_result_tx2);

        // Verify Tx2 commit failed with TransactionConflict (SSI Backward or standard OCC should catch this)
        assert!(commit_result_tx2.is_err());
        if let Err(KhonsuError::TransactionConflict) = commit_result_tx2 {
            println!(
                "Tx2 correctly failed with TransactionConflict (WW conflict with committed Tx1)"
            );
        } else {
            panic!("Tx2 failed with unexpected result: {:?}", commit_result_tx2);
        }

        // Verify the data in storage is still Tx1's data (Tx2 aborted)
        let mut verify_txn_final = khonsu.start_transaction();
        let final_stored_batch = verify_txn_final.read(&"key1".to_string()).unwrap().unwrap();
        assert_eq!(*final_stored_batch, record_batch_tx1);
        verify_txn_final.rollback();
    }

    #[test]
    fn test_dependency_removal_on_commit_and_abort() {
        let khonsu = setup_khonsu(TransactionIsolation::Serializable);

        let schema = create_basic_schema();
        let initial_record_batch =
            create_basic_record_batch(schema.clone(), vec!["key1"], vec![100]);

        // Write initial data with a separate transaction and commit
        let mut initial_txn = khonsu.start_transaction();
        initial_txn
            .write("key1".to_string(), initial_record_batch.clone())
            .unwrap();
        initial_txn.commit().unwrap();

        // Verify initial data is present by reading
        let mut verify_txn_initial = khonsu.start_transaction();
        assert!(verify_txn_initial
            .read(&"key1".to_string())
            .unwrap()
            .is_some());
        verify_txn_initial.rollback();

        // Start Tx1 (will commit) and Tx2 (will abort)
        let mut txn1 = khonsu.start_transaction();
        let txn1_id = txn1.id();
        let mut txn2 = khonsu.start_transaction();
        let txn2_id = txn2.id();

        // Tx1 reads key1
        txn1.read(&"key1".to_string())
            .expect("Error reading data in Tx1");
        println!("Tx1 ({}) read key1", txn1_id);

        // Tx2 writes key1
        let record_batch_tx2 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![500]);
        txn2.write("key1".to_string(), record_batch_tx2).unwrap();
        println!("Tx2 ({}) wrote key1", txn2_id);

        // Commit Tx1 (should succeed under SSI as no dangerous RW structure exists yet)
        println!("Attempting to commit Tx1 ({})", txn1_id);
        let commit_result_tx1 = txn1.commit();
        println!("Tx1 commit result: {:?}", commit_result_tx1);
        assert!(commit_result_tx1.is_ok()); // Expect Tx1 to succeed

        // Abort Tx2
        println!("Attempting to abort Tx2 ({})", txn2_id);
        txn2.rollback();
        println!("Tx2 aborted");

        // After Tx1 commits and Tx2 aborts, their dependencies should be removed from the tracker.
        // We can't directly inspect the SkipMap from here, but we can check if a new transaction
        // sees a clean state in terms of dependencies related to key1.
        // A more direct test would require exposing a method in DependencyTracker for testing,
        // but we'll rely on the logic within remove_transaction_dependencies for now.

        // Start a new transaction and perform an operation that would conflict if old dependencies existed.
        // If dependencies were not removed, a new transaction writing to key1 might see a false conflict.
        let mut txn3 = khonsu.start_transaction();
        let txn3_id = txn3.id();
        let record_batch_tx3 = create_basic_record_batch(schema.clone(), vec!["key1"], vec![600]);
        txn3.write("key1".to_string(), record_batch_tx3.clone())
            .unwrap(); // Clone batch for later assert
        println!("Tx3 ({}) wrote key1", txn3_id);

        // Commit Tx3 (should succeed if dependencies were removed)
        println!("Attempting to commit Tx3 ({})", txn3_id);
        let commit_result_tx3 = txn3.commit();
        println!("Tx3 commit result: {:?}", commit_result_tx3);
        assert!(commit_result_tx3.is_ok());

        // Verify the data in storage is now Tx3's data
        let mut verify_txn_final = khonsu.start_transaction();
        let final_stored_batch = verify_txn_final.read(&"key1".to_string()).unwrap().unwrap();
        assert_eq!(*final_stored_batch, record_batch_tx3);
        verify_txn_final.rollback();
    }
}
