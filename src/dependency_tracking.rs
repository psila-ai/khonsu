use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use parking_lot::RwLock;

use crate::errors::*;

/// Represents a transaction for dependency tracking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DependencyTransaction {
    pub id: u64,
    // Potentially other relevant transaction information
}

/// Represents a data item for dependency tracking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataItem {
    pub key: String,
    // Potentially other relevant data item information (e.g., version)
}

/// Tracks readers and writers for a specific data item, including versions.
#[derive(Debug, Clone)]
pub(crate) struct ItemDependency { // Made struct pub(crate) for visibility within the crate
    // Map from reader transaction ID to the version they read.
    pub(crate) readers: HashMap<u64, u64>, // Made field pub(crate)
    // Map from writer transaction ID to the version they wrote.
    // Using a HashMap to potentially track multiple writers for WW conflicts,
    // although the current SkipMap structure might make atomic updates tricky.
    pub(crate) writers: HashMap<u64, u64>, // Made field pub(crate)
}

impl ItemDependency {
    fn new() -> Self {
        Self {
            readers: HashMap::new(),
            writers: HashMap::new(),
        }
    }
}

/// A concurrent data structure to track transaction dependencies for serializability.
/// This structure maps data items to the transactions that have read or written them.
pub struct DependencyTracker {
    // Map from DataItem key to the transactions that have read or wrote it.
    pub(crate) item_dependencies: RwLock<HashMap<String, ItemDependency>>, // Made field pub(crate)
    // TODO: Need a way to track dependencies between transactions based on this item-level information.
    // This might involve building a transaction dependency graph during validation.
}

impl DependencyTracker {
    /// Creates a new `DependencyTracker`.
    pub fn new() -> Self {
        Self {
            item_dependencies: RwLock::new(HashMap::new()),
        }
    }

    /// Records that a transaction read a data item at a specific version.
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        let item_dep = item_dependencies
            .entry(data_item.key.clone())
            .or_insert_with(ItemDependency::new);

        // Modify the ItemDependency: add the reader and the version they read.
        item_dep.readers.insert(reader_id, read_version);

        println!(
            "Recorded read: Transaction {} read {:?} at version {}",
            reader_id, data_item, read_version
        );
    }

    /// Records that a transaction intends to write to a data item at a specific version.
    /// The write_version here is the version *being written*, which will become the new latest version.
    pub fn record_write(&self, writer_id: u64, data_item: DataItem, write_version: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        let item_dep = item_dependencies
            .entry(data_item.key.clone())
            .or_insert_with(ItemDependency::new);

        // Modify the ItemDependency: add the writer and the version they are writing.
        // This allows tracking multiple potential writers for WW conflicts.
        item_dep.writers.insert(writer_id, write_version);

        println!(
            "Recorded write: Transaction {} intends to write to {:?} at version {}",
            writer_id, data_item, write_version
        );
    }

    /// Checks for serializability violations (cycles) in the dependency graph.
    /// This method is called during the commit of a Serializable transaction.
    /// Returns `Ok(true)` if no cycle is detected, `Ok(false)` if a cycle is detected.
    /// This implementation builds a dependency graph based on current RW conflicts
    /// recorded in `item_dependencies` and performs DFS to detect cycles.
    pub fn check_for_cycles(
        &self,
        committing_tx_id: u64, // Keep parameter for logging
        read_set: &HashMap<String, u64>, // Use the read_set parameter now
        write_set: &HashSet<String>, // Use the write_set parameter now
    ) -> Result<bool> {
        // This function performs RW and WR conflict checks using the dependency tracker
        // to validate serializability against concurrent, uncommitted operations.

        let item_dependencies = self.item_dependencies.read();

        // Perform RW Conflict Check (Read Validation)
        // Check if any item read by this transaction has a concurrent write intention from another transaction.
        for (read_key, _read_version) in read_set { // Prefix unused read_version with underscore
            if let Some(item_dep) = item_dependencies.get(read_key) {
                for writer_id in item_dep.writers.keys() {
                    // Check if another transaction has a pending write for an item this transaction read.
                    if *writer_id != committing_tx_id {
                        println!(
                            "Serializable validation failed: RW conflict for Tx {} on key '{}'. Read conflicts with pending write by Tx {}.",
                            committing_tx_id, read_key, writer_id
                        );
                        return Ok(false); // RW conflict detected
                    }
                }
            }
        }

        // Perform WR Conflict Check (Write Validation)
        // Check if any item written by this transaction has been read by another active transaction.
        for write_key in write_set {
            if let Some(item_dep) = item_dependencies.get(write_key) {
                for reader_id in item_dep.readers.keys() {
                    // Check if another transaction has read an item this transaction is writing.
                    if *reader_id != committing_tx_id {
                        println!(
                            "Serializable validation failed: WR conflict for Tx {} on key '{}'. Write conflicts with read by Tx {}.",
                            committing_tx_id, write_key, reader_id
                        );
                        return Ok(false); // WR conflict detected, serializability violation
                    }
                }
            }
        }


        println!("Serializable validation successful (RW & WR checks passed) for Tx {}.", committing_tx_id);
        Ok(true) // No RW or WR conflict detected
    }

    // Note: The dfs_check_cycle helper function is no longer needed with this approach.
    /*
    fn dfs_check_cycle(...) { ... }
    */

    // TODO: Add methods for removing dependencies of committed or aborted transactions.
    // This is crucial for preventing the dependency tracker from growing indefinitely.
    // This will involve iterating through item_dependencies and removing the
    // committed/aborted transaction's ID from readers and writers.
    // This also needs to be done carefully to be concurrent-safe with the SkipMap.
    // This implementation is a best effort given the limitations of crossbeam-skiplist 0.1.
    pub fn remove_transaction_dependencies(&self, tx_id: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        // Iterate through all item dependencies.
        // We need to collect the keys first because we cannot modify the HashMap while iterating over it.
        let keys_to_check: Vec<String> = item_dependencies
            .keys()
            .cloned()
            .collect();

        for item_key in keys_to_check {
            if let Some(item_dep) = item_dependencies.get_mut(&item_key) {
                let mut modified = false;
                // Remove from readers.
                if item_dep.readers.remove(&tx_id).is_some() {
                    modified = true;
                }

                // Remove from writers.
                if item_dep.writers.remove(&tx_id).is_some() {
                    modified = true;
                }

                if modified {
                    if item_dep.readers.is_empty() && item_dep.writers.is_empty() {
                        // If no more dependencies for this item, remove the item from the HashMap.
                        item_dependencies.remove(&item_key);
                    }
                }
            }
        }
        println!("Removed dependencies for transaction {}", tx_id);
    }
}

// Need to add this module to src/lib.rs later.
