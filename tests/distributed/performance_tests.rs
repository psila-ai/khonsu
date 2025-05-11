#![cfg(feature = "distributed")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use khonsu::prelude::*;

use super::test_helpers::*;
use super::*;

/// Helper function to measure throughput
fn measure_throughput<F>(operation_count: usize, f: F) -> (Duration, f64)
where
    F: FnOnce(),
{
    let start = Instant::now();
    f();
    let elapsed = start.elapsed();
    let throughput = operation_count as f64 / elapsed.as_secs_f64();
    (elapsed, throughput)
}

#[test]
fn test_single_node_write_throughput() {
    // Set up a single-node distributed cluster
    let nodes = setup_distributed_cluster(1);
    let (khonsu, _temp_dir) = &nodes[0];

    // Number of operations to perform
    let operation_count = 1000;

    // Measure throughput of sequential writes
    let (elapsed, throughput) = measure_throughput(operation_count, || {
        for i in 0..operation_count {
            let mut txn = khonsu.start_transaction();
            let key = format!("perf_key_{}", i);
            let record_batch = create_test_record_batch(i as i32);
            txn.write(key, record_batch).unwrap();
            txn.commit().unwrap();
        }
    });

    println!(
        "Single-node sequential write throughput: {:.2} ops/sec ({} operations in {:.2?})",
        throughput, operation_count, elapsed
    );

    // Ensure throughput is reasonable (adjust threshold as needed)
    assert!(
        throughput > 10.0,
        "Single-node write throughput too low: {:.2} ops/sec",
        throughput
    );
}

#[test]
fn test_multi_node_write_throughput() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];

    // Number of operations to perform per node
    let operations_per_node = 300;
    let total_operations = operations_per_node * 3;

    // Measure throughput of parallel writes across nodes
    let success_count = Arc::new(AtomicUsize::new(0));

    let (elapsed, throughput) = measure_throughput(total_operations, || {
        let success_count1 = Arc::clone(&success_count);
        let success_count2 = Arc::clone(&success_count);
        let success_count3 = Arc::clone(&success_count);

        let khonsu1 = Arc::clone(khonsu1);
        let khonsu2 = Arc::clone(khonsu2);
        let khonsu3 = Arc::clone(khonsu3);

        // Start threads for each node
        let handle1 = thread::spawn(move || {
            for i in 0..operations_per_node {
                let mut txn = khonsu1.start_transaction();
                let key = format!("node1_key_{}", i);
                let record_batch = create_test_record_batch(i as i32);
                txn.write(key, record_batch).unwrap();
                if txn.commit().is_ok() {
                    success_count1.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        let handle2 = thread::spawn(move || {
            for i in 0..operations_per_node {
                let mut txn = khonsu2.start_transaction();
                let key = format!("node2_key_{}", i);
                let record_batch = create_test_record_batch(i as i32);
                txn.write(key, record_batch).unwrap();
                if txn.commit().is_ok() {
                    success_count2.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        let handle3 = thread::spawn(move || {
            for i in 0..operations_per_node {
                let mut txn = khonsu3.start_transaction();
                let key = format!("node3_key_{}", i);
                let record_batch = create_test_record_batch(i as i32);
                txn.write(key, record_batch).unwrap();
                if txn.commit().is_ok() {
                    success_count3.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        // Wait for all threads to complete
        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();
    });

    let successful_operations = success_count.load(Ordering::SeqCst);
    let success_rate = (successful_operations as f64 / total_operations as f64) * 100.0;

    println!("Multi-node parallel write throughput: {:.2} ops/sec ({}/{} operations in {:.2?}, success rate: {:.2}%)",
             throughput, successful_operations, total_operations, elapsed, success_rate);

    // Ensure throughput is reasonable and success rate is high
    assert!(
        throughput > 10.0,
        "Multi-node write throughput too low: {:.2} ops/sec",
        throughput
    );
    assert!(
        success_rate > 90.0,
        "Multi-node write success rate too low: {:.2}%",
        success_rate
    );
}

#[test]
fn test_read_throughput() {
    // Set up a two-node distributed cluster
    let nodes = setup_distributed_cluster(2);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];

    // Number of keys to create
    let key_count = 1000;

    // Create test data
    for i in 0..key_count {
        let mut txn = khonsu1.start_transaction();
        let key = format!("read_perf_key_{}", i);
        let record_batch = create_test_record_batch(i as i32);
        txn.write(key, record_batch).unwrap();
        txn.commit().unwrap();
    }

    // Manually replicate data
    for i in 0..key_count {
        let key = format!("read_perf_key_{}", i);
        manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key]);
    }

    // Measure read throughput on node 1 (local reads)
    let (elapsed1, throughput1) = measure_throughput(key_count, || {
        for i in 0..key_count {
            let mut txn = khonsu1.start_transaction();
            let key = format!("read_perf_key_{}", i);
            let result = txn.read(&key).unwrap();
            assert!(result.is_some());
        }
    });

    println!(
        "Local read throughput: {:.2} ops/sec ({} operations in {:.2?})",
        throughput1, key_count, elapsed1
    );

    // Measure read throughput on node 2 (remote reads)
    let (elapsed2, throughput2) = measure_throughput(key_count, || {
        for i in 0..key_count {
            let mut txn = khonsu2.start_transaction();
            let key = format!("read_perf_key_{}", i);
            let result = txn.read(&key).unwrap();
            assert!(result.is_some());
        }
    });

    println!(
        "Remote read throughput: {:.2} ops/sec ({} operations in {:.2?})",
        throughput2, key_count, elapsed2
    );

    // Ensure throughput is reasonable
    assert!(
        throughput1 > 100.0,
        "Local read throughput too low: {:.2} ops/sec",
        throughput1
    );
    assert!(
        throughput2 > 50.0,
        "Remote read throughput too low: {:.2} ops/sec",
        throughput2
    );
}

#[test]
fn test_mixed_workload_throughput() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, _temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];

    // Number of keys to create initially
    let initial_key_count = 500;

    // Create initial test data
    for i in 0..initial_key_count {
        let mut txn = khonsu1.start_transaction();
        let key = format!("mixed_key_{}", i);
        let record_batch = create_test_record_batch(i as i32);
        txn.write(key, record_batch).unwrap();
        txn.commit().unwrap();
    }

    // Manually replicate data
    for i in 0..initial_key_count {
        let key = format!("mixed_key_{}", i);
        manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);
        manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu3), &[key.clone()]);
    }

    // Number of operations for the mixed workload
    let operation_count = 1000;
    let read_ratio = 0.8; // 80% reads, 20% writes

    // Counters for successful operations
    let read_success = Arc::new(AtomicUsize::new(0));
    let write_success = Arc::new(AtomicUsize::new(0));

    // Measure throughput of mixed workload
    let (elapsed, throughput) = measure_throughput(operation_count, || {
        let read_success1 = Arc::clone(&read_success);
        let write_success1 = Arc::clone(&write_success);
        let read_success2 = Arc::clone(&read_success);
        let write_success2 = Arc::clone(&write_success);
        let read_success3 = Arc::clone(&read_success);
        let write_success3 = Arc::clone(&write_success);

        let khonsu1 = Arc::clone(khonsu1);
        let khonsu2 = Arc::clone(khonsu2);
        let khonsu3 = Arc::clone(khonsu3);

        let ops_per_node = operation_count / 3;

        // Start threads for each node
        let handle1 = thread::spawn(move || {
            for i in 0..ops_per_node {
                // Determine if this is a read or write operation
                let is_read = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as usize
                    % 100)
                    < (read_ratio * 100.0) as usize;

                if is_read {
                    // Read operation
                    let key_idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as usize)
                        % initial_key_count;
                    let key = format!("mixed_key_{}", key_idx);

                    let mut txn = khonsu1.start_transaction();
                    if txn.read(&key).is_ok() {
                        read_success1.fetch_add(1, Ordering::SeqCst);
                    }
                } else {
                    // Write operation
                    let key = format!("new_key_node1_{}", i);
                    let record_batch = create_test_record_batch(i as i32);

                    let mut txn = khonsu1.start_transaction();
                    txn.write(key, record_batch).unwrap();
                    if txn.commit().is_ok() {
                        write_success1.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        let handle2 = thread::spawn(move || {
            for i in 0..ops_per_node {
                // Determine if this is a read or write operation
                let is_read = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as usize
                    % 100)
                    < (read_ratio * 100.0) as usize;

                if is_read {
                    // Read operation
                    let key_idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as usize)
                        % initial_key_count;
                    let key = format!("mixed_key_{}", key_idx);

                    let mut txn = khonsu2.start_transaction();
                    if txn.read(&key).is_ok() {
                        read_success2.fetch_add(1, Ordering::SeqCst);
                    }
                } else {
                    // Write operation
                    let key = format!("new_key_node2_{}", i);
                    let record_batch = create_test_record_batch(i as i32);

                    let mut txn = khonsu2.start_transaction();
                    txn.write(key, record_batch).unwrap();
                    if txn.commit().is_ok() {
                        write_success2.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        let handle3 = thread::spawn(move || {
            for i in 0..ops_per_node {
                // Determine if this is a read or write operation
                let is_read = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as usize
                    % 100)
                    < (read_ratio * 100.0) as usize;

                if is_read {
                    // Read operation
                    let key_idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as usize)
                        % initial_key_count;
                    let key = format!("mixed_key_{}", key_idx);

                    let mut txn = khonsu3.start_transaction();
                    if txn.read(&key).is_ok() {
                        read_success3.fetch_add(1, Ordering::SeqCst);
                    }
                } else {
                    // Write operation
                    let key = format!("new_key_node3_{}", i);
                    let record_batch = create_test_record_batch(i as i32);

                    let mut txn = khonsu3.start_transaction();
                    txn.write(key, record_batch).unwrap();
                    if txn.commit().is_ok() {
                        write_success3.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        // Wait for all threads to complete
        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();
    });

    let total_reads = (operation_count as f64 * read_ratio) as usize;
    let total_writes = operation_count - total_reads;

    let successful_reads = read_success.load(Ordering::SeqCst);
    let successful_writes = write_success.load(Ordering::SeqCst);

    let read_success_rate = (successful_reads as f64 / total_reads as f64) * 100.0;
    let write_success_rate = (successful_writes as f64 / total_writes as f64) * 100.0;

    println!(
        "Mixed workload throughput: {:.2} ops/sec ({} operations in {:.2?})",
        throughput, operation_count, elapsed
    );
    println!(
        "Read success rate: {:.2}% ({}/{})",
        read_success_rate, successful_reads, total_reads
    );
    println!(
        "Write success rate: {:.2}% ({}/{})",
        write_success_rate, successful_writes, total_writes
    );

    // Ensure throughput and success rates are reasonable
    assert!(
        throughput > 10.0,
        "Mixed workload throughput too low: {:.2} ops/sec",
        throughput
    );
    assert!(
        read_success_rate > 90.0,
        "Read success rate too low: {:.2}%",
        read_success_rate
    );
    assert!(
        write_success_rate > 80.0,
        "Write success rate too low: {:.2}%",
        write_success_rate
    );
}
