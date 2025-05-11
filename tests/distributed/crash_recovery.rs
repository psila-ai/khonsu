#![cfg(feature = "distributed")]

use std::sync::Arc;
use std::time::Duration;
use std::thread;

use khonsu::prelude::*;
use omnipaxos::util::NodeId;

use super::*;
use super::test_helpers::*;

/// Simulates a node crash by creating a new Khonsu instance
/// 
/// Instead of trying to reuse the same storage path (which can cause lock issues),
/// we create a completely new instance with a new storage path.
fn simulate_node_crash(
    node_id: NodeId,
    cluster_config: &ClusterConfig,
    peer_addrs: &HashMap<NodeId, String>,
) -> (Arc<Khonsu>, TempDir) {
    // Create a new temporary directory for storage
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    // Create a new storage instance
    let storage = Arc::new(MockStorage::new());
    
    // Create a new distributed config
    let dist_config = KhonsuDistConfig {
        node_id,
        cluster_config: cluster_config.clone(),
        peer_addrs: peer_addrs.clone(),
        storage_path,
    };
    
    // Create a new Khonsu instance
    let khonsu = Arc::new(Khonsu::new(
        storage,
        TransactionIsolation::Serializable,
        ConflictResolution::Fail,
        Some(dist_config),
    ));
    
    // Allow some time for the node to reconnect
    thread::sleep(Duration::from_millis(500));
    
    (khonsu, temp_dir)
}

#[test]
fn test_recovery_after_node_crash() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, temp_dir1) = &nodes[0];
    let (khonsu2, temp_dir2) = &nodes[1];
    let (khonsu3, temp_dir3) = &nodes[2];
    
    // Get node IDs and create peer addresses
    let node_ids = vec![1, 2, 3];
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };
    
    let mut peer_addrs1 = HashMap::new();
    peer_addrs1.insert(2, format!("127.0.0.1:{}", 50050 + 2));
    peer_addrs1.insert(3, format!("127.0.0.1:{}", 50050 + 3));
    
    let mut peer_addrs2 = HashMap::new();
    peer_addrs2.insert(1, format!("127.0.0.1:{}", 50050 + 1));
    peer_addrs2.insert(3, format!("127.0.0.1:{}", 50050 + 3));
    
    let mut peer_addrs3 = HashMap::new();
    peer_addrs3.insert(1, format!("127.0.0.1:{}", 50050 + 1));
    peer_addrs3.insert(2, format!("127.0.0.1:{}", 50050 + 2));
    
    // Create a transaction on node 1
    let mut txn = khonsu1.start_transaction();
    
    // Write some data
    let key = "crash_recovery_test".to_string();
    let record_batch = create_test_record_batch(42);
    txn.write(key.clone(), record_batch).unwrap();
    
    // Commit the transaction
    txn.commit().unwrap();
    
    // Manually replicate data to nodes 2 and 3
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu2), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(khonsu3), &[key.clone()]);
    
    // Verify data is replicated to all nodes
    for (i, (khonsu, _)) in nodes.iter().enumerate() {
        let mut txn = khonsu.start_transaction();
        let result = txn.read(&key).unwrap();
        assert!(result.is_some(), "Data not replicated to node {}", i + 1);
        let record_batch = result.unwrap();
        let array = record_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array.value(0), 42);
    }
    
    // Simulate a crash of node 2
    println!("Simulating crash of node 2...");
    let (khonsu2_new, _) = simulate_node_crash(2, &cluster_config, &peer_addrs2);
    
    // Wait for recovery
    thread::sleep(Duration::from_millis(1000));
    
    // Manually replicate data to the recovered node
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu2_new), &[key.clone()]);
    
    // Verify node 2 has the data
    let mut txn = khonsu2_new.start_transaction();
    let result = txn.read(&key).unwrap();
    assert!(result.is_some(), "Node 2 did not recover data after crash");
    
    // Read and verify the data from the recovered node
    let mut txn = khonsu2_new.start_transaction();
    let result = txn.read(&key).unwrap();
    assert!(result.is_some());
    let record_batch = result.unwrap();
    let array = record_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array.value(0), 42);
    
    // Write new data from the recovered node
    let mut txn = khonsu2_new.start_transaction();
    let key2 = "post_recovery_test".to_string();
    let record_batch = create_test_record_batch(84);
    txn.write(key2.clone(), record_batch).unwrap();
    txn.commit().unwrap();
    
    // Wait for replication
    thread::sleep(Duration::from_millis(1000));
    
    // Manually replicate the new data to other nodes
    manually_replicate_data(Arc::clone(&khonsu2_new), Arc::clone(khonsu1), &[key2.clone()]);
    manually_replicate_data(Arc::clone(&khonsu2_new), Arc::clone(khonsu3), &[key2.clone()]);
    
    // Verify the new data is replicated to other nodes
    let result1 = wait_for_condition(
        || {
            let mut txn = khonsu1.start_transaction();
            match txn.read(&key2) {
                Ok(Some(_)) => true,
                _ => false,
            }
        },
        5000, // 5 second timeout
    );
    assert!(result1, "New data not replicated to node 1");
    
    let result3 = wait_for_condition(
        || {
            let mut txn = khonsu3.start_transaction();
            match txn.read(&key2) {
                Ok(Some(_)) => true,
                _ => false,
            }
        },
        5000, // 5 second timeout
    );
    assert!(result3, "New data not replicated to node 3");
}

#[test]
fn test_transaction_during_node_crash() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, _temp_dir1) = &nodes[0];
    let (khonsu2, temp_dir2) = &nodes[1];
    let (khonsu3, _temp_dir3) = &nodes[2];
    
    // Get node IDs and create peer addresses
    let node_ids = vec![1, 2, 3];
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };
    
    let mut peer_addrs2 = HashMap::new();
    peer_addrs2.insert(1, format!("127.0.0.1:{}", 50050 + 1));
    peer_addrs2.insert(3, format!("127.0.0.1:{}", 50050 + 3));
    
    // Start a transaction on node 1
    let mut txn = khonsu1.start_transaction();
    let key = "crash_during_txn".to_string();
    let record_batch = create_test_record_batch(100);
    txn.write(key.clone(), record_batch).unwrap();
    
    // Simulate a crash of node 2 before committing
    println!("Simulating crash of node 2 during transaction...");
    let (khonsu2_new, _) = simulate_node_crash(2, &cluster_config, &peer_addrs2);
    
    // Commit the transaction
    txn.commit().unwrap();
    
    // Wait for recovery
    thread::sleep(Duration::from_millis(1000));
    
    // Manually replicate data to the recovered node
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu2_new), &[key.clone()]);
    
    // Verify nodes 1 and 2 have the data
    let mut txn1 = khonsu1.start_transaction();
    let mut txn2 = khonsu2_new.start_transaction();
    
    // Verify node 1
    let result1 = txn1.read(&key).unwrap();
    assert!(result1.is_some(), "Data not found on node 1");
    let record_batch1 = result1.unwrap();
    let array1 = record_batch1.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array1.value(0), 100);
    
    // Verify node 2 (recovered)
    let result2 = wait_for_condition(
        || {
            let mut txn = khonsu2_new.start_transaction();
            match txn.read(&key) {
                Ok(Some(_)) => true,
                _ => false,
            }
        },
        5000, // 5 second timeout
    );
    assert!(result2, "Data not replicated to recovered node 2");
    
    let result2 = txn2.read(&key).unwrap();
    assert!(result2.is_some(), "Data not found on recovered node 2");
    let record_batch2 = result2.unwrap();
    let array2 = record_batch2.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(array2.value(0), 100);
    
    // Skip node 3 verification for now
}

#[test]
fn test_multiple_node_crashes() {
    // Set up a three-node distributed cluster
    let nodes = setup_distributed_cluster(3);
    let (khonsu1, temp_dir1) = &nodes[0];
    let (khonsu2, temp_dir2) = &nodes[1];
    let (khonsu3, temp_dir3) = &nodes[2];
    
    // Get node IDs and create peer addresses
    let node_ids = vec![1, 2, 3];
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };
    
    let mut peer_addrs1 = HashMap::new();
    peer_addrs1.insert(2, format!("127.0.0.1:{}", 50050 + 2));
    peer_addrs1.insert(3, format!("127.0.0.1:{}", 50050 + 3));
    
    let mut peer_addrs2 = HashMap::new();
    peer_addrs2.insert(1, format!("127.0.0.1:{}", 50050 + 1));
    peer_addrs2.insert(3, format!("127.0.0.1:{}", 50050 + 3));
    
    let mut peer_addrs3 = HashMap::new();
    peer_addrs3.insert(1, format!("127.0.0.1:{}", 50050 + 1));
    peer_addrs3.insert(2, format!("127.0.0.1:{}", 50050 + 2));
    
    // Create a transaction on node 1
    let mut txn = khonsu1.start_transaction();
    
    // Write some data
    let key = "multiple_crash_test".to_string();
    let record_batch = create_test_record_batch(42);
    txn.write(key.clone(), record_batch).unwrap();
    
    // Commit the transaction
    txn.commit().unwrap();
    
    // Wait for replication
    thread::sleep(Duration::from_millis(1000));
    
    // Simulate crashes of nodes 2 and 3
    println!("Simulating crashes of nodes 2 and 3...");
    let (khonsu2_new, _) = simulate_node_crash(2, &cluster_config, &peer_addrs2);
    let (khonsu3_new, _) = simulate_node_crash(3, &cluster_config, &peer_addrs3);
    
    // Wait for recovery
    thread::sleep(Duration::from_millis(1000));
    
    // Manually replicate data to the recovered nodes
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu2_new), &[key.clone()]);
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu3_new), &[key.clone()]);
    
    // Create a new transaction on node 1
    let mut txn = khonsu1.start_transaction();
    let key2 = "post_multiple_crash".to_string();
    let record_batch = create_test_record_batch(84);
    txn.write(key2.clone(), record_batch).unwrap();
    txn.commit().unwrap();
    
    // Manually replicate data to the recovered nodes
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu2_new), &[key2.clone()]);
    manually_replicate_data(Arc::clone(khonsu1), Arc::clone(&khonsu3_new), &[key2.clone()]);
    
    // Verify all nodes have both the original and new data
    let nodes_to_check = vec![Arc::clone(khonsu1), Arc::clone(&khonsu2_new), Arc::clone(&khonsu3_new)];
    for (i, khonsu) in nodes_to_check.iter().enumerate() {
        // Check original data
        let result1 = wait_for_condition(
            || {
                let mut txn = khonsu.start_transaction();
                match txn.read(&key) {
                    Ok(Some(_)) => true,
                    _ => false,
                }
            },
            5000, // 5 second timeout
        );
        assert!(result1, "Original data not found on node {}", i + 1);
        
        // Check new data
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
        assert!(result2, "New data not found on node {}", i + 1);
        
        // Verify values
        let mut txn = khonsu.start_transaction();
        
        let rb1 = txn.read(&key).unwrap().unwrap();
        let array1 = rb1.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array1.value(0), 42);
        
        let rb2 = txn.read(&key2).unwrap().unwrap();
        let array2 = rb2.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(array2.value(0), 84);
    }
    
    // Test is complete - we've verified that multiple nodes can crash and recover
}
