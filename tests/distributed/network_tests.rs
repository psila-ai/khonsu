#![cfg(feature = "distributed")]

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use khonsu::prelude::*;

use super::test_helpers::*;
use super::*;

#[test]
fn test_multi_node_communication() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];

    // Create a transaction on node 1
    let mut txn = khonsu1.start_transaction();

    // Write some data
    let key = "multi_node_test".to_string();
    let record_batch = create_test_record_batch(100);
    txn.write(key.clone(), record_batch).unwrap();

    // Commit the transaction
    txn.commit().unwrap();

    // Manually replicate data to nodes 2 and 3
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu3), &[key.clone()]);

    // Verify data is replicated to node 2
    let result2 = wait_for_condition(
        || {
            let mut txn = khonsu2.start_transaction();
            match txn.read(&key) {
                Ok(Some(_)) => true,
                _ => false,
            }
        },
        5000, // 5 second timeout
    );
    assert!(result2, "Data was not replicated to node 2 within timeout");

    // Verify data is replicated to node 3
    let result3 = wait_for_condition(
        || {
            let mut txn = khonsu3.start_transaction();
            match txn.read(&key) {
                Ok(Some(_)) => true,
                _ => false,
            }
        },
        5000, // 5 second timeout
    );
    assert!(result3, "Data was not replicated to node 3 within timeout");

    // Read and verify the data from all nodes
    let mut txn1 = khonsu1.start_transaction();
    let mut txn2 = khonsu2.start_transaction();
    let mut txn3 = khonsu3.start_transaction();

    let result1 = txn1.read(&key).unwrap().unwrap();
    let result2 = txn2.read(&key).unwrap().unwrap();
    let result3 = txn3.read(&key).unwrap().unwrap();

    let array1 = result1
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let array2 = result2
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let array3 = result3
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(array1.value(0), 100);
    assert_eq!(array2.value(0), 100);
    assert_eq!(array3.value(0), 100);
}

#[test]
fn test_sequential_transactions_across_nodes() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];

    // Transaction 1 on node 1
    let mut txn1 = khonsu1.start_transaction();
    let key = "sequential_test".to_string();
    let record_batch1 = create_test_record_batch(10);
    txn1.write(key.clone(), record_batch1).unwrap();
    txn1.commit().unwrap();

    // Manually replicate data to nodes 2 and 3
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu3), &[key.clone()]);

    // Transaction 2 on node 2
    let mut txn2 = khonsu2.start_transaction();
    let record_batch2 = create_test_record_batch(20);
    txn2.write(key.clone(), record_batch2).unwrap();
    txn2.commit().unwrap();

    // Manually replicate data to all nodes
    manually_replicate_data(Arc::clone(khonsu2), Arc::clone(khonsu1), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu2), Arc::clone(khonsu3), &[key.clone()]);

    // Transaction 3 on node 3
    let mut txn3 = khonsu3.start_transaction();
    let record_batch3 = create_test_record_batch(30);
    txn3.write(key.clone(), record_batch3).unwrap();
    txn3.commit().unwrap();

    // Manually replicate data to all nodes
    manually_replicate_data(Arc::clone(khonsu3), Arc::clone(khonsu1), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu3), Arc::clone(khonsu2), &[key.clone()]);

    // Verify final state on all nodes
    for (i, (khonsu, _)) in nodes.iter().enumerate() {
        let result = wait_for_condition(
            || {
                let mut txn = khonsu.start_transaction();
                match txn.read(&key) {
                    Ok(Some(rb)) => {
                        let array = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                        array.value(0) == 30
                    }
                    _ => false,
                }
            },
            5000, // 5 second timeout
        );
        assert!(result, "Node {} does not have the final value", i + 1);
    }
}

#[test]
fn test_concurrent_writes_from_different_nodes() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];

    // Create transactions on all nodes
    let mut txn1 = khonsu1.start_transaction();
    let mut txn2 = khonsu2.start_transaction();
    let mut txn3 = khonsu3.start_transaction();

    // Write to different keys
    let key1 = "concurrent_key1".to_string();
    let key2 = "concurrent_key2".to_string();
    let key3 = "concurrent_key3".to_string();

    let record_batch1 = create_test_record_batch(100);
    let record_batch2 = create_test_record_batch(200);
    let record_batch3 = create_test_record_batch(300);

    txn1.write(key1.clone(), record_batch1).unwrap();
    txn2.write(key2.clone(), record_batch2).unwrap();
    txn3.write(key3.clone(), record_batch3).unwrap();

    // Commit all transactions concurrently
    let handle1 = thread::spawn(move || txn1.commit().unwrap());
    let handle2 = thread::spawn(move || txn2.commit().unwrap());
    let handle3 = thread::spawn(move || txn3.commit().unwrap());

    // Wait for all commits to complete
    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();

    // Manually replicate data
    manually_replicate_data(
        Arc::clone(khonsu1),
        Arc::clone(khonsu2),
        &[key1.clone(), key2.clone(), key3.clone()],
    );
    manually_replicate_data(
        Arc::clone(khonsu2),
        Arc::clone(khonsu1),
        &[key1.clone(), key2.clone(), key3.clone()],
    );
    manually_replicate_data(
        Arc::clone(khonsu3),
        Arc::clone(khonsu1),
        &[key1.clone(), key2.clone(), key3.clone()],
    );
    manually_replicate_data(
        Arc::clone(khonsu1),
        Arc::clone(khonsu3),
        &[key1.clone(), key2.clone(), key3.clone()],
    );
    manually_replicate_data(
        Arc::clone(khonsu2),
        Arc::clone(khonsu3),
        &[key1.clone(), key2.clone(), key3.clone()],
    );
    manually_replicate_data(
        Arc::clone(khonsu3),
        Arc::clone(khonsu2),
        &[key1.clone(), key2.clone(), key3.clone()],
    );

    // Verify all data is replicated to all nodes
    for (i, (khonsu, _)) in nodes.iter().enumerate() {
        // Check key1
        let result1 = wait_for_condition(
            || {
                let mut txn = khonsu.start_transaction();
                match txn.read(&key1) {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            },
            5000, // 5 second timeout
        );
        assert!(result1, "Key1 not replicated to node {}", i + 1);

        // Check key2
        let result2 = wait_for_condition(
            || {
                let mut txn = khonsu.start_transaction();
                match txn.read(&key2) {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            },
            5000, // 5 second timeout
        );
        assert!(result2, "Key2 not replicated to node {}", i + 1);

        // Check key3
        let result3 = wait_for_condition(
            || {
                let mut txn = khonsu.start_transaction();
                match txn.read(&key3) {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            },
            5000, // 5 second timeout
        );
        assert!(result3, "Key3 not replicated to node {}", i + 1);

        // Verify values
        let mut txn = khonsu.start_transaction();

        let rb1 = txn.read(&key1).unwrap().unwrap();
        let array1 = rb1.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array1.value(0), 100);

        let rb2 = txn.read(&key2).unwrap().unwrap();
        let array2 = rb2.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array2.value(0), 200);

        let rb3 = txn.read(&key3).unwrap().unwrap();
        let array3 = rb3.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array3.value(0), 300);
    }
}

#[test]
fn test_read_your_writes_consistency() {
    // Set up a two-node distributed cluster
    let nodes = setup_distributed_cluster(2);
    let (khonsu1, _temp_dir1) = &nodes[0];

    // Create a transaction and write data
    let mut txn1 = khonsu1.start_transaction();
    let key = "read_your_writes".to_string();
    let record_batch = create_test_record_batch(42);
    txn1.write(key.clone(), record_batch).unwrap();

    // Read the data within the same transaction
    let result = txn1.read(&key).unwrap();
    assert!(result.is_some());
    let record_batch = result.unwrap();
    let array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(array.value(0), 42);

    // Commit the transaction
    txn1.commit().unwrap();

    // Create a new transaction and verify the data is visible
    let mut txn2 = khonsu1.start_transaction();
    let result = txn2.read(&key).unwrap();
    assert!(result.is_some());
    let record_batch = result.unwrap();
    let array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(array.value(0), 42);
}

#[test]
fn test_transaction_ordering() {
    // Set up a two-node distributed cluster
    let nodes = setup_distributed_cluster(2);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];

    // Create a sequence of transactions on different nodes
    let key = "ordering_test".to_string();

    // Transaction 1 on node 1
    let mut txn1 = khonsu1.start_transaction();
    let record_batch1 = create_test_record_batch(10);
    txn1.write(key.clone(), record_batch1).unwrap();
    txn1.commit().unwrap();

    // Manually replicate data
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);

    // Transaction 2 on node 2
    let mut txn2 = khonsu2.start_transaction();
    let record_batch2 = create_test_record_batch(20);
    txn2.write(key.clone(), record_batch2).unwrap();
    txn2.commit().unwrap();

    // Manually replicate data
    manually_replicate_data(Arc::clone(khonsu2), Arc::clone(khonsu1), &[key.clone()]);

    // Transaction 3 on node 1
    let mut txn3 = khonsu1.start_transaction();
    let record_batch3 = create_test_record_batch(30);
    txn3.write(key.clone(), record_batch3).unwrap();
    txn3.commit().unwrap();

    // Manually replicate data
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);

    // Verify that both nodes see the same final value
    let mut verify_txn1 = khonsu1.start_transaction();
    let mut verify_txn2 = khonsu2.start_transaction();

    let result1 = verify_txn1.read(&key).unwrap();
    let result2 = verify_txn2.read(&key).unwrap();

    assert!(result1.is_some());
    assert!(result2.is_some());

    let rb1 = result1.unwrap();
    let rb2 = result2.unwrap();

    let array1 = rb1.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let array2 = rb2.column(0).as_any().downcast_ref::<Int32Array>().unwrap();

    assert_eq!(array1.value(0), 30);
    assert_eq!(array2.value(0), 30);
}
