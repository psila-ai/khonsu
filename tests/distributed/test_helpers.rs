use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use khonsu::data_store::versioned_value::VersionedValue;
use khonsu::Khonsu;

/// Helper function to manually replicate data between nodes for testing.
///
/// This function is used in tests to simulate data replication between nodes
/// without relying on the actual network communication.
///
/// # Arguments
///
/// * `source_khonsu` - The source Khonsu instance.
/// * `target_khonsu` - The target Khonsu instance.
/// * `keys` - The keys to replicate.
pub fn manually_replicate_data(
    source_khonsu: Arc<Khonsu>,
    target_khonsu: Arc<Khonsu>,
    keys: &[String],
) {
    // Read data from source node
    let mut source_txn = source_khonsu.start_transaction();
    let mut data_to_replicate = HashMap::new();

    for key in keys {
        if let Ok(Some(record_batch)) = source_txn.read(key) {
            data_to_replicate.insert(key.clone(), record_batch);
        }
    }
    source_txn.rollback();

    // Write data to target node
    let mut target_txn = target_khonsu.start_transaction();
    for (key, record_batch) in data_to_replicate {
        let _ = target_txn.write(key, (*record_batch).clone());
    }
    let _ = target_txn.commit();

    // Give some time for the commit to complete
    std::thread::sleep(Duration::from_millis(100));
}

/// Helper function to manually check if data exists on a node.
///
/// This function is used in tests to check if data exists on a node
/// without relying on the actual network communication.
///
/// # Arguments
///
/// * `khonsu` - The Khonsu instance to check.
/// * `key` - The key to check.
///
/// # Returns
///
/// `true` if the key exists, `false` otherwise.
pub fn data_exists_on_node(khonsu: &Arc<Khonsu>, key: &str) -> bool {
    let mut txn = khonsu.start_transaction();
    let result = txn.read(&key.to_string()).unwrap().is_some();
    txn.rollback();
    result
}

/// Helper function to manually get data from a node.
///
/// This function is used in tests to get data from a node
/// without relying on the actual network communication.
///
/// # Arguments
///
/// * `khonsu` - The Khonsu instance to get data from.
/// * `key` - The key to get.
///
/// # Returns
///
/// The record batch if it exists, `None` otherwise.
pub fn get_data_from_node(khonsu: &Arc<Khonsu>, key: &str) -> Option<Arc<RecordBatch>> {
    let mut txn = khonsu.start_transaction();
    let result = txn.read(&key.to_string()).unwrap();
    txn.rollback();
    result
}

/// Helper function to manually set data on a node.
///
/// This function is used in tests to set data on a node
/// without relying on the actual network communication.
///
/// # Arguments
///
/// * `khonsu` - The Khonsu instance to set data on.
/// * `key` - The key to set.
/// * `record_batch` - The record batch to set.
pub fn set_data_on_node(khonsu: &Arc<Khonsu>, key: &str, record_batch: RecordBatch) {
    let mut txn = khonsu.start_transaction();
    let _ = txn.write(key.to_string(), record_batch);
    let _ = txn.commit();
}
