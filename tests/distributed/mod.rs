#![cfg(feature = "distributed")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use khonsu::prelude::*;
use omnipaxos::ClusterConfig;
use omnipaxos::util::NodeId;
use tempfile::TempDir;

// Import KhonsuDistConfig directly from the distributed module
use crate::distributed::dist_config::KhonsuDistConfig;

// Export test modules
pub mod basic_distributed;
pub mod consistency_tests;
pub mod crash_recovery;
pub mod network_tests;
pub mod performance_tests;
pub mod test_helpers;

/// Helper function to create a test record batch with a single integer value
pub fn create_test_record_batch(value: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let value_array = Int32Array::from(vec![value]);
    RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap()
}

/// Helper function to set up a distributed cluster for testing
pub fn setup_distributed_cluster(node_count: usize) -> Vec<(Arc<Khonsu>, TempDir)> {
    let mut result = Vec::with_capacity(node_count);
    let mut peer_addrs = HashMap::new();
    let mut node_ids = Vec::with_capacity(node_count);

    // Generate node IDs and peer addresses
    for i in 1..=node_count {
        let node_id = i as NodeId;
        node_ids.push(node_id);
        peer_addrs.insert(node_id, format!("127.0.0.1:{}", 50050 + node_id));
    }

    // Create cluster config
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };

    // Create Khonsu instances for each node
    for node_id in node_ids.iter().cloned() {
        // Create a temporary directory for RocksDB storage
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().to_path_buf();

        // Create peer addresses map (excluding self)
        let mut node_peer_addrs = HashMap::new();
        for &peer_id in &node_ids {
            if peer_id != node_id {
                node_peer_addrs.insert(peer_id, peer_addrs[&peer_id].clone());
            }
        }

        // Create distributed config
        let dist_config = KhonsuDistConfig {
            node_id,
            cluster_config: cluster_config.clone(),
            peer_addrs: node_peer_addrs,
            storage_path,
        };

        // Create storage
        let storage = Arc::new(MockStorage::new());

        // Create Khonsu instance
        let khonsu = Arc::new(Khonsu::new(
            storage,
            TransactionIsolation::Serializable,
            ConflictResolution::Fail,
            Some(dist_config),
        ));

        result.push((khonsu, temp_dir));
    }

    // Allow some time for the nodes to connect
    std::thread::sleep(Duration::from_millis(500));

    result
}

/// A mock storage implementation for testing
pub struct MockStorage {
    data: parking_lot::Mutex<HashMap<String, RecordBatch>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            data: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<RecordBatch> {
        let data = self.data.lock();
        data.get(key).cloned()
    }
}

impl Storage for MockStorage {
    fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
        let mut data = self.data.lock();
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

/// Helper function to wait for a condition with timeout
pub fn wait_for_condition<F>(condition: F, timeout_ms: u64) -> bool
where
    F: Fn() -> bool,
{
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms);

    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    false
}
