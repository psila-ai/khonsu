use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering}; // Needed for cleanup counter
use std::time::{Duration, Instant}; // Needed for cleanup timing

use crate::data_store::txn_buffer::TxnBuffer; // Keep this import
use crate::errors::*;

/// Represents the state of a transaction being tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// Holds information about a transaction relevant for SSI validation.
#[derive(Debug, Clone)]
pub(crate) struct TransactionInfo {
    pub(crate) state: TxnState,
    pub(crate) start_ts: u64, // Transaction ID acts as start timestamp
    pub(crate) commit_ts: Option<u64>,
    // Keep track of writes for forward validation against committed transactions
    // Only needed for recently committed transactions, can be cleared during cleanup.
    pub(crate) write_set_keys: Option<HashSet<String>>,
    pub(crate) creation_time: Instant, // For cleanup purposes
}

/// Represents a data item for dependency tracking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataItem {
    pub key: String,
    // Potentially other relevant data item information (e.g., version)
}

/// Tracks readers and writers for a specific data item, including versions.
#[derive(Debug, Clone, Default)]
pub(crate) struct ItemDependency {
    // Map from *active* reader transaction ID to the version they read.
    pub(crate) readers: HashMap<u64, u64>,
    // Map from *active* writer transaction ID. Value is not currently used but key presence indicates active write intention.
    pub(crate) writers: HashSet<u64>, // Changed to HashSet
}

/// A concurrent data structure to track transaction dependencies for SSI validation.
pub struct DependencyTracker {
    // Tracks state (Active/Committed/Aborted) and timestamps for transactions.
    // Key: Transaction ID (start_ts)
    transactions: RwLock<HashMap<u64, TransactionInfo>>,
    // Tracks which *active* transactions are reading/writing specific items.
    // Key: Data item key (String)
    item_dependencies: RwLock<HashMap<String, ItemDependency>>,
    // Counter for triggering cleanup periodically
    cleanup_counter: AtomicU64,
    // Threshold for triggering cleanup
    cleanup_threshold: u64,
    // Max age for keeping committed/aborted transaction info (especially write sets)
    max_txn_age: Duration,
}

impl DependencyTracker {
    /// Creates a new `DependencyTracker`.
    pub fn new() -> Self {
        Self {
            transactions: RwLock::new(HashMap::new()),
            item_dependencies: RwLock::new(HashMap::new()),
            cleanup_counter: AtomicU64::new(0),
            cleanup_threshold: 1000, // Example: Cleanup every 1000 operations
            max_txn_age: Duration::from_secs(60), // Example: Keep info for 60 seconds
        }
    }

    /// Registers a new transaction as Active.
    pub fn register_txn(&self, txn_id: u64) {
        let info = TransactionInfo {
            state: TxnState::Active,
            start_ts: txn_id,
            commit_ts: None,
            write_set_keys: None, // Write set only added on commit
            creation_time: Instant::now(),
        };
        self.transactions.write().insert(txn_id, info);
        println!("Registered Tx {} as Active", txn_id);
        self.maybe_trigger_cleanup(); // Periodically cleanup
    }

    /// Marks a transaction as Committed. Stores write set keys for recent commits.
    pub fn mark_committed(&self, txn_id: u64, commit_ts: u64, write_set_keys: HashSet<String>) {
        if let Some(info) = self.transactions.write().get_mut(&txn_id) {
            info.state = TxnState::Committed;
            info.commit_ts = Some(commit_ts);
            info.write_set_keys = Some(write_set_keys); // Store write set for recent commits
            println!("Marked Tx {} as Committed (CommitTS: {})", txn_id, commit_ts);
        } else {
            println!("Warning: Tx {} not found in tracker to mark as Committed.", txn_id);
        }
        self.remove_active_dependencies(txn_id);
        self.maybe_trigger_cleanup();
    }

    /// Marks a transaction as Aborted.
    pub fn mark_aborted(&self, txn_id: u64) {
        if let Some(info) = self.transactions.write().get_mut(&txn_id) {
            info.state = TxnState::Aborted;
            // No need to store write set for aborted transactions
            info.write_set_keys = None;
            println!("Marked Tx {} as Aborted", txn_id);
        } else {
             println!("Warning: Tx {} not found in tracker to mark as Aborted.", txn_id);
        }
        self.remove_active_dependencies(txn_id);
        self.maybe_trigger_cleanup();
    }


    /// Records that an *active* transaction read a data item at a specific version.
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        if self.transactions.read().get(&reader_id).map_or(false, |info| info.state == TxnState::Active) {
            let mut item_deps = self.item_dependencies.write();
            let item_dep = item_deps
                .entry(data_item.key.clone())
                .or_insert_with(ItemDependency::default);
            item_dep.readers.insert(reader_id, read_version);
            println!(
                "Recorded active read: Tx {} read {:?} at version {}",
                reader_id, data_item, read_version
            );
        }
    }

    /// Records that an *active* transaction intends to write to a data item.
    pub fn record_write(&self, writer_id: u64, data_item: DataItem, _write_version: u64) {
         if self.transactions.read().get(&writer_id).map_or(false, |info| info.state == TxnState::Active) {
            let mut item_deps = self.item_dependencies.write();
            let item_dep = item_deps
                .entry(data_item.key.clone())
                .or_insert_with(ItemDependency::default);
            item_dep.writers.insert(writer_id); // Just track the active writer ID
            println!(
                "Recorded active write intention: Tx {} intends to write to {:?}",
                writer_id, data_item
            );
        }
    }

    /// Validates serializability by checking for direct conflicts with other *active* transactions.
    /// Returns `Ok(true)` if no direct conflicts found, `Ok(false)` otherwise.
    pub fn validate_serializability(
        &self,
        committing_tx_id: u64,
        _commit_ts: u64, // Parameter unused in this version
        read_set: &HashMap<String, u64>, // Read set of the committing transaction
        write_set: &HashSet<String>,     // Write set of the committing transaction
        _txn_buffer: &TxnBuffer, // Parameter unused in this version
    ) -> Result<bool> {
        println!("Starting serializability validation (Active Conflict Check) for Tx {}", committing_tx_id);
        let item_deps = self.item_dependencies.read(); // Read lock on item dependencies
        let transactions_read_lock = self.transactions.read(); // Read lock on transactions

        // Check for Write-Read conflicts: Fail if any item being written was read by another active transaction.
        for write_key in write_set {
            if let Some(dep) = item_deps.get(write_key) {
                for reader_id in dep.readers.keys() {
                    if *reader_id != committing_tx_id {
                        // Check if reader is still active
                        if transactions_read_lock.get(reader_id).map_or(false, |info| info.state == TxnState::Active) {
                            println!(
                                "Serializable validation failed: WR conflict for Tx {} on key '{}'. Write conflicts with active read by Tx {}.",
                                committing_tx_id, write_key, reader_id
                            );
                            return Ok(false); // Direct WR conflict with active transaction
                        }
                    }
                }
            }
        }

        // Check for Write-Write conflicts: Fail if any item being written was also written by another active transaction.
        for write_key in write_set {
             if let Some(dep) = item_deps.get(write_key) {
                 for writer_id in &dep.writers { // Iterate HashSet
                    if *writer_id != committing_tx_id {
                         // Check if writer is still active
                         if transactions_read_lock.get(writer_id).map_or(false, |info| info.state == TxnState::Active) {
                             println!(
                                "Serializable validation failed: WW conflict for Tx {} on key '{}'. Write conflicts with active write by Tx {}.",
                                committing_tx_id, write_key, writer_id
                            );
                            return Ok(false); // Direct WW conflict with active transaction
                         }
                    }
                }
            }
        }

        // Check for Read-Write conflicts: Fail if any item read was written by another active transaction.
        for read_key in read_set.keys() {
            if let Some(dep) = item_deps.get(read_key) {
                for writer_id in &dep.writers { // Iterate HashSet
                    if *writer_id != committing_tx_id {
                         // Check if writer is still active
                         if transactions_read_lock.get(writer_id).map_or(false, |info| info.state == TxnState::Active) {
                             println!(
                                "Serializable validation failed: RW conflict for Tx {} on key '{}'. Read conflicts with active write by Tx {}.",
                                committing_tx_id, read_key, writer_id
                            );
                            return Ok(false); // Direct RW conflict with active transaction
                         }
                    }
                }
            }
        }

        // If no direct conflicts found with active transactions, validation passes.
        println!("Serializable validation (Active Conflict Check) successful for Tx {}", committing_tx_id);
        Ok(true)
    }


    /// Removes a transaction's read/write entries from the item_dependencies map.
    /// Called after commit or abort.
    fn remove_active_dependencies(&self, tx_id: u64) {
        let mut item_dependencies = self.item_dependencies.write();
        let keys_to_check: Vec<String> = item_dependencies.keys().cloned().collect();

        for item_key in keys_to_check {
            if let Some(item_dep) = item_dependencies.get_mut(&item_key) {
                let mut modified = false;
                if item_dep.readers.remove(&tx_id).is_some() {
                    modified = true;
                }
                // Use remove for HashSet
                if item_dep.writers.remove(&tx_id) {
                    modified = true;
                }
                // If item has no more active readers/writers, remove it from the map
                if modified && item_dep.readers.is_empty() && item_dep.writers.is_empty() {
                    item_dependencies.remove(&item_key);
                }
            }
        }
        println!("Removed active dependencies for transaction {}", tx_id);
    }

    /// Periodically checks and removes old transaction info.
    fn maybe_trigger_cleanup(&self) {
         let count = self.cleanup_counter.fetch_add(1, Ordering::Relaxed);
         if count >= self.cleanup_threshold {
             self.cleanup_counter.store(0, Ordering::Relaxed); // Reset counter
             self.cleanup_old_txns();
         }
    }

    /// Removes info for transactions older than max_txn_age.
    fn cleanup_old_txns(&self) {
        let now = Instant::now();
        let mut transactions = self.transactions.write();
        let initial_count = transactions.len();
        transactions.retain(|_tx_id, info| {
            // Keep Active transactions and recently Committed/Aborted ones
            // Clear write_set_keys for older committed transactions to save memory
            if info.state != TxnState::Active && now.duration_since(info.creation_time) >= self.max_txn_age {
                return false; // Remove old committed/aborted info entirely
            }
            true // Keep active and recent committed/aborted
        });
        let removed_count = initial_count - transactions.len();
        if removed_count > 0 {
            println!("Cleaned up info for {} old transactions.", removed_count);
        }
    }
}
