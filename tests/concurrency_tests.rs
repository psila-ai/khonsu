// Declare the common module *within this test crate*
mod common;

use std::sync::{Arc, Barrier};
use std::thread;

// Use khonsu:: prefix for library items
use khonsu::{Khonsu, TransactionIsolation, KhonsuError};
// Use common:: prefix for shared test utilities (since `mod common;` is declared above)
use common::{setup_khonsu, create_record_batch};


#[test]
fn test_serializable_rw_conflict_interleaved() {
    // Test Scenario: R-W conflict under Serializable isolation using threads
    // 1. Initial: Write key1 = 100
    // 2. Tx1 (Thread 1): Start, Read key1, Wait(B1), Wait(B2), Commit
    // 3. Tx2 (Thread 2): Wait(B1), Start, Write key1 = 300, Commit, Signal(B2)
    // Expected: Tx1 reads 100. Tx2 commits successfully. Tx1 commit fails (backward validation).

    // Setup Khonsu with Serializable isolation
    let khonsu_arc = setup_khonsu(TransactionIsolation::Serializable);
    // khonsu_arc.set_default_isolation(TransactionIsolation::Serializable); // Remove this line

    // 1. Initial data setup
    let initial_batch = create_record_batch(vec![1], vec![Some("initial")]);
    let mut setup_txn = khonsu_arc.start_transaction();
    setup_txn.write("key1".to_string(), initial_batch.clone()).unwrap();
    setup_txn.commit().unwrap();
    println!("Initial data committed.");

    let barrier = Arc::new(Barrier::new(2)); // Barrier for 2 threads

    // Clone barrier for Thread 1
    let barrier_tx1 = barrier.clone();
    let khonsu_tx1 = khonsu_arc.clone();

    // Clone barrier for Thread 2
    let barrier_tx2 = barrier.clone(); // This clone is for Thread 2
    let khonsu_tx2 = khonsu_arc.clone();

    // Thread 1 (Tx1 - Reader)
    let handle1 = thread::spawn(move || { // barrier_tx1 is moved here
        let mut txn1 = khonsu_tx1.start_transaction();
        let txn1_id = txn1.id();
        println!("Tx1 ({}) started.", txn1_id);

        // Read initial data
        let read_result = txn1.read(&"key1".to_string());
        println!("Tx1 ({}) read key1.", txn1_id);
        assert!(read_result.is_ok(), "Tx1 read failed");
        let read_batch = read_result.unwrap().expect("Tx1 should find key1");
        assert_eq!(*read_batch, initial_batch, "Tx1 read wrong initial value");

        // Wait for Tx2 to start and write
        println!("Tx1 ({}) waiting at Barrier 1.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1 ({}) passed Barrier 1.", txn1_id);

        // Wait for Tx2 to commit
        println!("Tx1 ({}) waiting at Barrier 2.", txn1_id);
        barrier_tx1.wait(); // Use barrier_tx1 here
        println!("Tx1 ({}) passed Barrier 2.", txn1_id);

        // Attempt to commit Tx1 (should fail due to conflict with committed Tx2)
        println!("Tx1 ({}) attempting commit.", txn1_id);
        let commit_result = txn1.commit();
        println!("Tx1 ({:?}) commit result: {:?}", commit_result, txn1_id); // Use {:?} for Result

        // Assert Tx1 commit failed due to TransactionConflict (SSI backward validation)
        assert!(commit_result.is_err());
        match commit_result.err().unwrap() {
            KhonsuError::TransactionConflict => println!("Tx1 ({}) correctly failed with TransactionConflict.", txn1_id),
            e => panic!("Tx1 ({}) failed with unexpected error: {:?}", txn1_id, e),
        }
    });

    // Thread 2 (Tx2 - Writer)
    let handle2 = thread::spawn(move || { // barrier_tx2 is moved here
        // Wait for Tx1 to read
        println!("Tx2 waiting at Barrier 1.");
        barrier_tx2.wait(); // Use barrier_tx2 here
        println!("Tx2 passed Barrier 1.");

        let mut txn2 = khonsu_tx2.start_transaction();
        let txn2_id = txn2.id();
        println!("Tx2 ({}) started.", txn2_id);

        // Write new data
        let write_batch = create_record_batch(vec![1], vec![Some("updated_by_tx2")]);
        txn2.write("key1".to_string(), write_batch.clone()).unwrap();
        println!("Tx2 ({}) wrote key1.", txn2_id);

        // Attempt to commit Tx2 (should succeed as Tx1 hasn't committed yet)
        println!("Tx2 ({}) attempting commit.", txn2_id);
        let commit_result = txn2.commit();
        println!("Tx2 ({:?}) commit result: {:?}", commit_result, txn2_id); // Use {:?} for Result
        assert!(commit_result.is_ok(), "Tx2 commit failed unexpectedly");

        // Signal Tx1 to proceed
        println!("Tx2 ({}) waiting at Barrier 2 (after commit).", txn2_id);
        barrier_tx2.wait(); // Use barrier_tx2 here
        println!("Tx2 ({}) passed Barrier 2.", txn2_id);

        // Return the committed batch for verification
        write_batch
    });

    // Wait for threads to complete
    handle1.join().expect("Thread 1 panicked");
    let final_batch_from_tx2 = handle2.join().expect("Thread 2 panicked");

    // Verify final state in storage (should be Tx2's write)
    let mut final_read_txn = khonsu_arc.start_transaction();
    let final_read_result = final_read_txn.read(&"key1".to_string()).unwrap().unwrap();
    assert_eq!(*final_read_result, final_batch_from_tx2, "Final data in storage is incorrect");
    println!("Final data verified in storage.");
}


#[test]
fn test_serializable_ww_conflict_interleaved() {
    // Test Scenario: W-W conflict under Serializable isolation using threads
    // 1. Initial: (Optional) Write key1 = initial
    // 2. Tx1 (Thread 1): Start, Write key1 = tx1_val, Wait(B1), Wait(B2), Commit
    // 3. Tx2 (Thread 2): Wait(B1), Start, Write key1 = tx2_val, Commit, Signal(B2)
    // Expected: Tx1 commits successfully. Tx2 commit fails (WW conflict).

    let khonsu_arc = setup_khonsu(TransactionIsolation::Serializable);

    // Optional: Initial data setup (can also start empty)
    let initial_batch = create_record_batch(vec![1], vec![Some("initial_ww")]);
    let mut setup_txn = khonsu_arc.start_transaction();
    setup_txn.write("key1_ww".to_string(), initial_batch.clone()).unwrap();
    setup_txn.commit().unwrap();
    println!("Initial WW data committed.");


    let barrier = Arc::new(Barrier::new(2)); // Barrier for 2 threads

    // Clone resources for Thread 1
    let barrier_tx1 = barrier.clone();
    let khonsu_tx1 = khonsu_arc.clone();

    // Clone resources for Thread 2
    let barrier_tx2 = barrier.clone();
    let khonsu_tx2 = khonsu_arc.clone();

    // Thread 1 (Tx1 - First Writer)
    let handle1 = thread::spawn(move || {
        let mut txn1 = khonsu_tx1.start_transaction();
        let txn1_id = txn1.id();
        println!("Tx1-WW ({}) started.", txn1_id);

        // Write new data
        let write_batch_tx1 = create_record_batch(vec![1], vec![Some("updated_by_tx1_ww")]);
        txn1.write("key1_ww".to_string(), write_batch_tx1.clone()).unwrap();
        println!("Tx1-WW ({}) wrote key1_ww.", txn1_id);

        // Wait for Tx2 to start and write
        println!("Tx1-WW ({}) waiting at Barrier 1.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-WW ({}) passed Barrier 1.", txn1_id);

        // Wait for Tx2 to attempt commit
        println!("Tx1-WW ({}) waiting at Barrier 2.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-WW ({}) passed Barrier 2.", txn1_id);

        // Attempt to commit Tx1 (should succeed as it was the first writer to reach commit phase implicitly)
        println!("Tx1-WW ({}) attempting commit.", txn1_id);
        let commit_result = txn1.commit();
        println!("Tx1-WW ({:?}) commit result: {:?}", commit_result, txn1_id); // Already correct {:?}
        assert!(commit_result.is_ok(), "Tx1-WW commit failed unexpectedly");

        // Return the committed batch for verification
        write_batch_tx1
    });

    // Thread 2 (Tx2 - Second Writer)
    let handle2 = thread::spawn(move || {
        // Wait for Tx1 to write
        println!("Tx2-WW waiting at Barrier 1.");
        barrier_tx2.wait();
        println!("Tx2-WW passed Barrier 1.");

        let mut txn2 = khonsu_tx2.start_transaction();
        let txn2_id = txn2.id();
        println!("Tx2-WW ({}) started.", txn2_id);

        // Write different data
        let write_batch_tx2 = create_record_batch(vec![1], vec![Some("updated_by_tx2_ww")]);
        txn2.write("key1_ww".to_string(), write_batch_tx2.clone()).unwrap();
        println!("Tx2-WW ({}) wrote key1_ww.", txn2_id);

        // Attempt to commit Tx2 (should fail due to WW conflict with Tx1)
        println!("Tx2-WW ({}) attempting commit.", txn2_id);
        let commit_result = txn2.commit();
        println!("Tx2-WW ({:?}) commit result: {:?}", commit_result, txn2_id); // Already correct {:?}

        // Signal Tx1 to proceed (regardless of commit outcome)
        println!("Tx2-WW ({}) waiting at Barrier 2 (after commit attempt).", txn2_id);
        barrier_tx2.wait();
        println!("Tx2-WW ({}) passed Barrier 2.", txn2_id);

        // Assert Tx2 commit failed due to TransactionConflict
        assert!(commit_result.is_err());
        match commit_result.err().unwrap() {
            KhonsuError::TransactionConflict => println!("Tx2-WW ({}) correctly failed with TransactionConflict.", txn2_id),
            e => panic!("Tx2-WW ({}) failed with unexpected error: {:?}", txn2_id, e),
        }
    });

    // Wait for threads to complete
    let final_batch_from_tx1 = handle1.join().expect("Thread 1 panicked");
    handle2.join().expect("Thread 2 panicked");

    // Verify final state in storage (should be Tx1's write)
    let mut final_read_txn = khonsu_arc.start_transaction();
    let final_read_result = final_read_txn.read(&"key1_ww".to_string()).unwrap().unwrap();
    assert_eq!(*final_read_result, final_batch_from_tx1, "Final data in storage is incorrect (should be Tx1's write)");
    println!("Final WW data verified in storage.");
}


#[test]
fn test_serializable_wr_conflict_interleaved() {
    // Test Scenario: W-R conflict under Serializable isolation using threads
    // 1. Initial: Write key1 = initial
    // 2. Tx1 (Thread 1): Start, Write key1 = tx1_val, Wait(B1), Commit, Signal(B2)
    // 3. Tx2 (Thread 2): Wait(B1), Start, Read key1 (sees initial), Wait(B2), Commit
    // Expected: Tx1 commits successfully. Tx2 commit fails (WR conflict - backward validation).

    let khonsu_arc = setup_khonsu(TransactionIsolation::Serializable);

    // Initial data setup
    let initial_batch = create_record_batch(vec![1], vec![Some("initial_wr")]);
    let mut setup_txn = khonsu_arc.start_transaction();
    setup_txn.write("key1_wr".to_string(), initial_batch.clone()).unwrap();
    setup_txn.commit().unwrap();
    println!("Initial WR data committed.");

    let barrier = Arc::new(Barrier::new(2)); // Barrier for 2 threads

    // Clone resources for Thread 1 (Writer)
    let barrier_tx1 = barrier.clone();
    let khonsu_tx1 = khonsu_arc.clone();

    // Clone resources for Thread 2 (Reader)
    let barrier_tx2 = barrier.clone();
    let khonsu_tx2 = khonsu_arc.clone();

    // Thread 1 (Tx1 - Writer)
    let handle1 = thread::spawn(move || {
        let mut txn1 = khonsu_tx1.start_transaction();
        let txn1_id = txn1.id();
        println!("Tx1-WR ({}) started.", txn1_id);

        // Write new data
        let write_batch_tx1 = create_record_batch(vec![1], vec![Some("updated_by_tx1_wr")]);
        txn1.write("key1_wr".to_string(), write_batch_tx1.clone()).unwrap();
        println!("Tx1-WR ({}) wrote key1_wr.", txn1_id);

        // Wait for Tx2 to start and read
        println!("Tx1-WR ({}) waiting at Barrier 1.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-WR ({}) passed Barrier 1.", txn1_id);

        // Attempt to commit Tx1 (should succeed)
        println!("Tx1-WR ({}) attempting commit.", txn1_id);
        let commit_result = txn1.commit();
        println!("Tx1-WR ({:?}) commit result: {:?}", commit_result, txn1_id); // Already correct {:?}
        assert!(commit_result.is_ok(), "Tx1-WR commit failed unexpectedly");

        // Signal Tx2 to proceed after commit
        println!("Tx1-WR ({}) waiting at Barrier 2 (after commit).", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-WR ({}) passed Barrier 2.", txn1_id);

        // Return the committed batch for verification
        write_batch_tx1
    });

    // Thread 2 (Tx2 - Reader)
    let handle2 = thread::spawn(move || {
        // Wait for Tx1 to write (but before commit)
        println!("Tx2-WR waiting at Barrier 1.");
        barrier_tx2.wait();
        println!("Tx2-WR passed Barrier 1.");

        let mut txn2 = khonsu_tx2.start_transaction();
        let txn2_id = txn2.id();
        println!("Tx2-WR ({}) started.", txn2_id);

        // Read data (should see initial value as Tx1 hasn't committed yet)
        let read_result = txn2.read(&"key1_wr".to_string());
         println!("Tx2-WR ({}) read key1_wr.", txn2_id);
        assert!(read_result.is_ok(), "Tx2-WR read failed");
        let read_batch = read_result.unwrap().expect("Tx2-WR should find key1_wr");
        assert_eq!(*read_batch, initial_batch, "Tx2-WR read wrong initial value");


        // Wait for Tx1 to commit
        println!("Tx2-WR ({}) waiting at Barrier 2.", txn2_id);
        barrier_tx2.wait();
        println!("Tx2-WR ({}) passed Barrier 2.", txn2_id);

        // Attempt to commit Tx2 (should fail due to reading stale data - WR conflict)
        println!("Tx2-WR ({}) attempting commit.", txn2_id);
        let commit_result = txn2.commit();
        println!("Tx2-WR ({:?}) commit result: {:?}", commit_result, txn2_id); // Already correct {:?}

        // Assert Tx2 commit failed due to TransactionConflict (SSI backward validation)
        assert!(commit_result.is_err());
        match commit_result.err().unwrap() {
            KhonsuError::TransactionConflict => println!("Tx2-WR ({}) correctly failed with TransactionConflict.", txn2_id),
            e => panic!("Tx2-WR ({}) failed with unexpected error: {:?}", txn2_id, e),
        }
    });

    // Wait for threads to complete
    let final_batch_from_tx1 = handle1.join().expect("Thread 1 panicked");
    handle2.join().expect("Thread 2 panicked");

    // Verify final state in storage (should be Tx1's write)
    let mut final_read_txn = khonsu_arc.start_transaction();
    let final_read_result = final_read_txn.read(&"key1_wr".to_string()).unwrap().unwrap();
    assert_eq!(*final_read_result, final_batch_from_tx1, "Final data in storage is incorrect (should be Tx1's write)");
    println!("Final WR data verified in storage.");
}


#[test]
fn test_serializable_multi_key_rw_cycle_interleaved() {
    // Test Scenario: R-W cycle across multiple keys under Serializable isolation
    // 1. Initial: Write key_a = initial_a, key_b = initial_b
    // 2. Tx1 (Thread 1): Start, Read key_a, Wait(B1), Write key_b = tx1_b, Wait(B2), Commit
    // 3. Tx2 (Thread 2): Wait(B1), Start, Read key_b, Wait(B2), Write key_a = tx2_a, Commit
    // Expected: One transaction must abort to break the cycle. Which one depends on timing,
    //           but SSI should prevent both from committing. Let's aim for Tx2 aborting.

    let khonsu_arc = setup_khonsu(TransactionIsolation::Serializable);

    // Initial data setup
    let initial_batch_a = create_record_batch(vec![1], vec![Some("initial_a")]);
    let initial_batch_b = create_record_batch(vec![2], vec![Some("initial_b")]); // Use different ID/value for clarity
    let mut setup_txn = khonsu_arc.start_transaction();
    setup_txn.write("key_a".to_string(), initial_batch_a.clone()).unwrap();
    setup_txn.write("key_b".to_string(), initial_batch_b.clone()).unwrap();
    setup_txn.commit().unwrap();
    println!("Initial RW-Cycle data committed.");

    let barrier = Arc::new(Barrier::new(2)); // Barrier for 2 threads

    // Clone resources for Thread 1
    let barrier_tx1 = barrier.clone();
    let khonsu_tx1 = khonsu_arc.clone();

    // Clone resources for Thread 2
    let barrier_tx2 = barrier.clone();
    let khonsu_tx2 = khonsu_arc.clone();

    let iba = initial_batch_a.clone();
    // Thread 1 (Tx1: R(a) -> W(b))
    let handle1 = thread::spawn(move || {
        let mut txn1 = khonsu_tx1.start_transaction();
        let txn1_id = txn1.id();
        println!("Tx1-Cycle ({}) started.", txn1_id);

        // Read key_a
        let read_result_a = txn1.read(&"key_a".to_string());
        println!("Tx1-Cycle ({}) read key_a.", txn1_id);
        assert!(read_result_a.is_ok(), "Tx1-Cycle read key_a failed");
        let read_batch_a = read_result_a.unwrap().expect("Tx1-Cycle should find key_a");
        assert_eq!(*read_batch_a, iba, "Tx1-Cycle read wrong initial value for key_a");

        // Wait for Tx2 to start and read key_b
        println!("Tx1-Cycle ({}) waiting at Barrier 1.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-Cycle ({}) passed Barrier 1.", txn1_id);

        // Write key_b
        let write_batch_b_tx1 = create_record_batch(vec![2], vec![Some("updated_b_by_tx1")]);
        txn1.write("key_b".to_string(), write_batch_b_tx1.clone()).unwrap();
        println!("Tx1-Cycle ({}) wrote key_b.", txn1_id);

        // Wait for Tx2 to write key_a
        println!("Tx1-Cycle ({}) waiting at Barrier 2.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-Cycle ({}) passed Barrier 2.", txn1_id);

        // Attempt to commit Tx1 (might succeed or fail depending on exact timing vs Tx2 commit attempt)
        println!("Tx1-Cycle ({}) attempting commit.", txn1_id);
        let commit_result = txn1.commit();
        println!("Tx1-Cycle ({:?}) commit result: {:?}", commit_result, txn1_id);

        // Return result and potentially committed batch
        (commit_result, write_batch_b_tx1)
    });


    let ibb = initial_batch_b.clone();
    // Thread 2 (Tx2: R(b) -> W(a))
    let handle2 = thread::spawn(move || {
        // Wait for Tx1 to read key_a
        println!("Tx2-Cycle waiting at Barrier 1.");
        barrier_tx2.wait();
        println!("Tx2-Cycle passed Barrier 1.");

        let mut txn2 = khonsu_tx2.start_transaction();
        let txn2_id = txn2.id();
        println!("Tx2-Cycle ({}) started.", txn2_id);

        // Read key_b
        let read_result_b = txn2.read(&"key_b".to_string());
        println!("Tx2-Cycle ({}) read key_b.", txn2_id);
        assert!(read_result_b.is_ok(), "Tx2-Cycle read key_b failed");
        let read_batch_b = read_result_b.unwrap().expect("Tx2-Cycle should find key_b");
        assert_eq!(*read_batch_b, ibb, "Tx2-Cycle read wrong initial value for key_b");

        // Wait for Tx1 to write key_b
        println!("Tx2-Cycle ({}) waiting at Barrier 2.", txn2_id);
        barrier_tx2.wait();
        println!("Tx2-Cycle ({}) passed Barrier 2.", txn2_id);

        // Write key_a
        let write_batch_a_tx2 = create_record_batch(vec![1], vec![Some("updated_a_by_tx2")]);
        txn2.write("key_a".to_string(), write_batch_a_tx2.clone()).unwrap();
        println!("Tx2-Cycle ({}) wrote key_a.", txn2_id);

        // Attempt to commit Tx2 (might succeed or fail depending on exact timing vs Tx1 commit attempt)
        println!("Tx2-Cycle ({}) attempting commit.", txn2_id);
        let commit_result = txn2.commit();
        println!("Tx2-Cycle ({:?}) commit result: {:?}", commit_result, txn2_id);

        // Return result and potentially committed batch
        (commit_result, write_batch_a_tx2)
    });

    // Wait for threads to complete
    let (result1, batch1) = handle1.join().expect("Thread 1 panicked");
    let (result2, batch2) = handle2.join().expect("Thread 2 panicked");

    // Assert that AT LEAST ONE transaction failed due to conflict
    assert!(result1.is_err() || result2.is_err(), "At least one transaction should have failed due to RW cycle conflict");

    if result1.is_err() {
        println!("Tx1-Cycle correctly failed.");
        assert!(matches!(result1.err().unwrap(), KhonsuError::TransactionConflict));
        // If Tx1 failed, Tx2 should have succeeded
        assert!(result2.is_ok(), "If Tx1 failed, Tx2 should have succeeded");
        // Verify final state: key_a = tx2, key_b = initial_b
        let mut final_read_txn = khonsu_arc.start_transaction();
        let final_a = final_read_txn.read(&"key_a".to_string()).unwrap().unwrap();
        let final_b = final_read_txn.read(&"key_b".to_string()).unwrap().unwrap();
        assert_eq!(*final_a, batch2, "Final key_a should be from Tx2");
        assert_eq!(*final_b, initial_batch_b, "Final key_b should be initial");
    } else {
        println!("Tx1-Cycle succeeded.");
        // If Tx1 succeeded, Tx2 should have failed
        assert!(result2.is_err(), "If Tx1 succeeded, Tx2 should have failed");
        assert!(matches!(result2.err().unwrap(), KhonsuError::TransactionConflict));
        // Verify final state: key_a = initial_a, key_b = tx1
        let mut final_read_txn = khonsu_arc.start_transaction();
        let final_a = final_read_txn.read(&"key_a".to_string()).unwrap().unwrap();
        let final_b = final_read_txn.read(&"key_b".to_string()).unwrap().unwrap();
        assert_eq!(*final_a, initial_batch_a, "Final key_a should be initial");
        assert_eq!(*final_b, batch1, "Final key_b should be from Tx1");
    }

    println!("Final RW-Cycle data verified in storage.");
}


#[test]
fn test_serializable_ssi_forward_validation_interleaved() {
    // Test Scenario: SSI Forward Validation (Dangerous Structure R->W->R)
    // 1. Initial: Write key_x = initial_x, key_y = initial_y
    // 2. Tx1 (Thread 1): Start, Read key_x, Wait(B1), Wait(B2), Write key_y = tx1_y, Wait(B3), Commit
    // 3. Tx2 (Thread 2): Wait(B1), Start, Write key_x = tx2_x, Commit, Signal(B2)
    // 4. Tx3 (Thread 3): Wait(B3), Start, Read key_y, Commit
    // Expected: Tx2 commits. Tx3 commits (reading initial_y or tx1_y depending on timing).
    //           Tx1 should ABORT due to the dangerous R(x) -> W(x by Tx2) -> R(y by Tx3) structure,
    //           where Tx1's write to y depends on its read of x, and Tx3 reads y after Tx2 committed.

    let khonsu_arc = setup_khonsu(TransactionIsolation::Serializable);

    // Initial data setup
    let initial_batch_x = create_record_batch(vec![10], vec![Some("initial_x")]);
    let initial_batch_y = create_record_batch(vec![20], vec![Some("initial_y")]);
    let mut setup_txn = khonsu_arc.start_transaction();
    setup_txn.write("key_x".to_string(), initial_batch_x.clone()).unwrap();
    setup_txn.write("key_y".to_string(), initial_batch_y.clone()).unwrap();
    setup_txn.commit().unwrap();
    println!("Initial SSI-Fwd data committed.");

    let barrier = Arc::new(Barrier::new(3)); // Barrier for 3 threads

    // Clone resources for Thread 1 (R(x) -> W(y))
    let barrier_tx1 = barrier.clone();
    let khonsu_tx1 = khonsu_arc.clone();

    // Clone resources for Thread 2 (W(x))
    let barrier_tx2 = barrier.clone();
    let khonsu_tx2 = khonsu_arc.clone();

    // Clone resources for Thread 3 (R(y))
    let barrier_tx3 = barrier.clone();
    let khonsu_tx3 = khonsu_arc.clone();

    // Thread 1 (Tx1: R(x) -> W(y))
    let handle1 = thread::spawn(move || {
        let mut txn1 = khonsu_tx1.start_transaction();
        let txn1_id = txn1.id();
        println!("Tx1-Fwd ({}) started.", txn1_id);

        // Read key_x
        let read_result_x = txn1.read(&"key_x".to_string());
        println!("Tx1-Fwd ({}) read key_x.", txn1_id);
        assert!(read_result_x.is_ok(), "Tx1-Fwd read key_x failed");
        let _read_batch_x = read_result_x.unwrap().expect("Tx1-Fwd should find key_x");
        // assert_eq!(*_read_batch_x, initial_batch_x, "Tx1-Fwd read wrong initial value for key_x"); // Value doesn't matter, just the read dependency

        // Wait for Tx2 to start and write key_x
        println!("Tx1-Fwd ({}) waiting at Barrier 1.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-Fwd ({}) passed Barrier 1.", txn1_id);

        // Wait for Tx2 to commit
        println!("Tx1-Fwd ({}) waiting at Barrier 2.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-Fwd ({}) passed Barrier 2.", txn1_id);

        // Write key_y (conditionally based on read of x)
        let write_batch_y_tx1 = create_record_batch(vec![20], vec![Some("updated_y_by_tx1")]);
        txn1.write("key_y".to_string(), write_batch_y_tx1.clone()).unwrap();
        println!("Tx1-Fwd ({}) wrote key_y.", txn1_id);

        // Wait for Tx3 to start and read key_y
        println!("Tx1-Fwd ({}) waiting at Barrier 3.", txn1_id);
        barrier_tx1.wait();
        println!("Tx1-Fwd ({}) passed Barrier 3.", txn1_id);

        // Attempt to commit Tx1 (should fail due to forward validation)
        println!("Tx1-Fwd ({}) attempting commit.", txn1_id);
        let commit_result = txn1.commit();
        println!("Tx1-Fwd ({:?}) commit result: {:?}", commit_result, txn1_id);

        // Assert Tx1 commit failed due to TransactionConflict (SSI Forward Validation)
        assert!(commit_result.is_err());
        match commit_result.err().unwrap() {
            KhonsuError::TransactionConflict => println!("Tx1-Fwd ({}) correctly failed with TransactionConflict (Forward Validation).", txn1_id),
            e => panic!("Tx1-Fwd ({}) failed with unexpected error: {:?}", txn1_id, e),
        }
    });

    // Thread 2 (Tx2: W(x))
    let handle2 = thread::spawn(move || {
        // Wait for Tx1 to read key_x
        println!("Tx2-Fwd waiting at Barrier 1.");
        barrier_tx2.wait();
        println!("Tx2-Fwd passed Barrier 1.");

        let mut txn2 = khonsu_tx2.start_transaction();
        let txn2_id = txn2.id();
        println!("Tx2-Fwd ({}) started.", txn2_id);

        // Write key_x
        let write_batch_x_tx2 = create_record_batch(vec![10], vec![Some("updated_x_by_tx2")]);
        txn2.write("key_x".to_string(), write_batch_x_tx2.clone()).unwrap();
        println!("Tx2-Fwd ({}) wrote key_x.", txn2_id);

        // Attempt to commit Tx2 (should succeed)
        println!("Tx2-Fwd ({}) attempting commit.", txn2_id);
        let commit_result = txn2.commit();
        println!("Tx2-Fwd ({:?}) commit result: {:?}", commit_result, txn2_id);
        assert!(commit_result.is_ok(), "Tx2-Fwd commit failed unexpectedly");

        // Signal Tx1 to proceed after commit
        println!("Tx2-Fwd ({}) waiting at Barrier 2 (after commit).", txn2_id);
        barrier_tx2.wait();
        println!("Tx2-Fwd ({}) passed Barrier 2.", txn2_id);

        // Wait for Tx1 to write key_y and Tx3 to start
        println!("Tx2-Fwd ({}) waiting at Barrier 3.", txn2_id);
        barrier_tx2.wait();
        println!("Tx2-Fwd ({}) passed Barrier 3.", txn2_id);

        // Return committed batch
        write_batch_x_tx2
    });

    // Thread 3 (Tx3: R(y))
    let handle3 = thread::spawn(move || {
         // Wait for Tx1 and Tx2 to pass Barrier 2 (Tx2 committed, Tx1 wrote y)
        println!("Tx3-Fwd waiting at Barrier 1.", ); // No wait needed here
        barrier_tx3.wait(); // Sync point 1
        println!("Tx3-Fwd passed Barrier 1.", );
        barrier_tx3.wait(); // Sync point 2
        println!("Tx3-Fwd passed Barrier 2.", );


        // Wait for Tx1 to write key_y (but before Tx1 commit attempt)
        println!("Tx3-Fwd waiting at Barrier 3.", );
        barrier_tx3.wait();
        println!("Tx3-Fwd passed Barrier 3.", );

        let mut txn3 = khonsu_tx3.start_transaction();
        let txn3_id = txn3.id();
        println!("Tx3-Fwd ({}) started.", txn3_id);

        // Read key_y (might see initial_y or tx1's write depending on exact timing relative to Tx1's write)
        // The important part is that this read happens *after* Tx2 commits.
        let read_result_y = txn3.read(&"key_y".to_string());
        println!("Tx3-Fwd ({}) read key_y.", txn3_id);
        assert!(read_result_y.is_ok(), "Tx3-Fwd read key_y failed");
        let _read_batch_y = read_result_y.unwrap().expect("Tx3-Fwd should find key_y");

        // Attempt to commit Tx3 (should succeed)
        println!("Tx3-Fwd ({}) attempting commit.", txn3_id);
        let commit_result = txn3.commit();
        println!("Tx3-Fwd ({:?}) commit result: {:?}", commit_result, txn3_id);
        assert!(commit_result.is_ok(), "Tx3-Fwd commit failed unexpectedly");

    });


    // Wait for threads to complete
    handle1.join().expect("Thread 1 panicked");
    let final_batch_x = handle2.join().expect("Thread 2 panicked");
    handle3.join().expect("Thread 3 panicked");


    // Verify final state in storage
    // Tx1 aborted, Tx2 committed, Tx3 committed.
    // key_x should be Tx2's write.
    // key_y should be the initial value (since Tx1 aborted).
    let mut final_read_txn = khonsu_arc.start_transaction();
    let final_x = final_read_txn.read(&"key_x".to_string()).unwrap().unwrap();
    let final_y = final_read_txn.read(&"key_y".to_string()).unwrap().unwrap();
    assert_eq!(*final_x, final_batch_x, "Final key_x should be from Tx2");
    assert_eq!(*final_y, initial_batch_y, "Final key_y should be initial (Tx1 aborted)");

    println!("Final SSI-Fwd data verified in storage.");
}
