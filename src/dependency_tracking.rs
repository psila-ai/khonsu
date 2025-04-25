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
    // Set of *active* writer transaction IDs.
    pub(crate) writers: HashSet<u64>,
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
        let found = {
            // Scope the write lock
            let mut transactions_guard = self.transactions.write();
            if let Some(info) = transactions_guard.get_mut(&txn_id) {
                info.state = TxnState::Committed;
                info.commit_ts = Some(commit_ts);
                info.write_set_keys = Some(write_set_keys); // Store write set for recent commits
                println!(
                    "Marked Tx {} as Committed (CommitTS: {})",
                    txn_id, commit_ts
                );
                true
            } else {
                println!(
                    "Warning: Tx {} not found in tracker to mark as Committed.",
                    txn_id
                );
                false
            }
            // transactions_guard lock is released here
        };

        if found {
            self.remove_active_dependencies(txn_id); // Acquire item_dependencies lock *after* releasing transactions lock
        }
        self.maybe_trigger_cleanup();
    }

    /// Marks a transaction as Aborted.
    pub fn mark_aborted(&self, txn_id: u64) {
        let found = {
            // Scope the write lock
            let mut transactions_guard = self.transactions.write();
            if let Some(info) = transactions_guard.get_mut(&txn_id) {
                info.state = TxnState::Aborted;
                info.write_set_keys = None; // Clear write set on abort
                println!("Marked Tx {} as Aborted", txn_id);
                true
            } else {
                println!(
                    "Warning: Tx {} not found in tracker to mark as Aborted.",
                    txn_id
                );
                false
            }
            // transactions_guard lock is released here
        };

        if found {
            self.remove_active_dependencies(txn_id); // Acquire item_dependencies lock *after* releasing transactions lock
        }
        self.maybe_trigger_cleanup();
    }

    /// Records that an *active* transaction read a data item at a specific version.
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        if self
            .transactions
            .read()
            .get(&reader_id)
            .map_or(false, |info| info.state == TxnState::Active)
        {
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
        if self
            .transactions
            .read()
            .get(&writer_id)
            .map_or(false, |info| info.state == TxnState::Active)
        {
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

    /// Validates serializability using SSI principles.
    pub fn validate_serializability(
        &self,
        committing_tx_id: u64,
        _commit_ts: u64, // commit_ts is not strictly needed for basic SI backward check
        read_set: &HashMap<String, u64>, // Read set of the committing transaction
        write_set: &HashSet<String>, // Write set of the committing transaction
        txn_buffer: &TxnBuffer, // Need buffer for backward validation
    ) -> Result<bool> {
        println!("Starting SSI validation for Tx {}", committing_tx_id);
        let transactions_read_lock = self.transactions.read();

        let start_ts = match transactions_read_lock.get(&committing_tx_id) {
            Some(info) if info.state == TxnState::Active => info.start_ts,
            _ => {
                println!(
                    "Error: Committing Tx {} not found or not active.",
                    committing_tx_id
                );
                return Ok(false); // Cannot validate non-active transaction
            }
        };

        // --- Backward Validation ---
        // 1. SI Read Check: Check if any item read by T_commit was overwritten by a transaction T_other
        //    that committed *after* T_commit started.
        for (key, read_version) in read_set {
            if let Some(current_value) = txn_buffer.get(key) {
                if current_value.version() > *read_version {
                    let writer_commit_ts = current_value.version();
                    if writer_commit_ts > start_ts {
                        println!("SSI Backward Validation Failed (RW): Tx {} read key '{}' at ver {}, but Tx {} committed ver {} at TS {}",
                                   committing_tx_id, key, read_version, writer_commit_ts, writer_commit_ts, writer_commit_ts);
                        return Ok(false);
                    }
                }
            } else {
                // Item we read was deleted. Conflict if deletion committed after we started.
                if *read_version > 0 {
                    // Heuristic: If we read version > 0, it existed
                    println!("SSI Backward Validation Failed (RD Heuristic): Tx {} read key '{}' at ver {}, but it was deleted.",
                              committing_tx_id, key, read_version);
                    return Ok(false);
                }
            }
        }

        // 2. WW Check: Check if any item written by T_commit was also written by a transaction T_other
        //    that committed *after* T_commit started.
        for key in write_set {
            if let Some(current_value) = txn_buffer.get(key) {
                let writer_commit_ts = current_value.version();
                println!(
                    "  SSI WW Check: Key='{}', CommittingTx={}, StartTs={}, BufferVersion={}",
                    key, committing_tx_id, start_ts, writer_commit_ts
                ); // ADDED LOG
                   // Conflict if writer committed after we started.
                if writer_commit_ts > start_ts {
                    println!("  SSI Backward Validation Failed (WW): Tx {} writing key '{}', but Tx {} committed ver {} at TS {}",
                               committing_tx_id, key, writer_commit_ts, writer_commit_ts, writer_commit_ts);
                    return Ok(false);
                }
            }
            // If current_value is None, it means either the key never existed or was deleted.
            // An insert by T_commit is okay. A write/delete by T_commit on a deleted item
            // might conflict if the deletion happened after start_ts, but needs deletion tracking.
        }

        // --- Forward Validation (RW-Dependency Check) ---
        let item_deps_read_lock = self.item_dependencies.read();
        let mut has_outgoing_rw_edge = false; // T_commit writes y <- T_active reads y
        let mut has_incoming_rw_edge = false; // T_commit reads x <- T_concurrent writes x

        // Check for outgoing edges: T_commit writes y, T_active reads y
        for write_key in write_set {
            if let Some(dep) = item_deps_read_lock.get(write_key) {
                for reader_id in dep.readers.keys() {
                    if *reader_id != committing_tx_id {
                        if transactions_read_lock
                            .get(reader_id)
                            .map_or(false, |info| info.state == TxnState::Active)
                        {
                            println!("SSI Forward Validation: Found outgoing RW edge from Tx {} to active Tx {} on key '{}'", committing_tx_id, reader_id, write_key);
                            has_outgoing_rw_edge = true;
                            if has_incoming_rw_edge {
                                break;
                            } // Optimization
                        }
                    }
                }
                if has_outgoing_rw_edge && has_incoming_rw_edge {
                    break;
                } // Optimization
            }
        }

        // Check for incoming edges: T_commit reads x, T_concurrent writes x
        // T_concurrent can be Active or Committed (after T_commit started)
        if !(has_outgoing_rw_edge && has_incoming_rw_edge) {
            // Skip if already found dangerous structure
            for read_key in read_set.keys() {
                // Check active writers
                if let Some(dep) = item_deps_read_lock.get(read_key) {
                    for writer_id in &dep.writers {
                        if *writer_id != committing_tx_id {
                            if transactions_read_lock
                                .get(writer_id)
                                .map_or(false, |info| info.state == TxnState::Active)
                            {
                                println!("SSI Forward Validation: Found incoming RW edge from active Tx {} to Tx {} on key '{}'", writer_id, committing_tx_id, read_key);
                                has_incoming_rw_edge = true;
                                if has_outgoing_rw_edge {
                                    break;
                                } // Optimization
                            }
                        }
                    }
                }
                if has_outgoing_rw_edge && has_incoming_rw_edge {
                    break;
                } // Optimization

                // Check committed writers (after T_commit started)
                for (other_txn_id, other_info) in transactions_read_lock.iter() {
                    if *other_txn_id != committing_tx_id
                        && other_info.state == TxnState::Committed
                        && other_info.commit_ts.unwrap_or(0) > start_ts
                    {
                        if let Some(ref committed_write_set) = other_info.write_set_keys {
                            if committed_write_set.contains(read_key) {
                                println!("SSI Forward Validation: Found incoming RW edge from committed Tx {} to Tx {} on key '{}'", other_txn_id, committing_tx_id, read_key);
                                has_incoming_rw_edge = true;
                                if has_outgoing_rw_edge {
                                    break;
                                } // Optimization
                            }
                        }
                    }
                }
                if has_outgoing_rw_edge && has_incoming_rw_edge {
                    break;
                } // Optimization
            }
        }
        drop(item_deps_read_lock);
        drop(transactions_read_lock); // Drop the lock now

        // Check for dangerous structure (SIREAD + PREAD conflict)
        if has_incoming_rw_edge && has_outgoing_rw_edge {
            println!(
                "SSI Forward Validation Failed: Dangerous RW structure detected for Tx {}",
                committing_tx_id
            );
            return Ok(false);
        }

        println!("SSI validation successful for Tx {}", committing_tx_id);
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
                if item_dep.writers.remove(&tx_id) {
                    modified = true;
                }
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
            if info.state != TxnState::Active
                && now.duration_since(info.creation_time) >= self.max_txn_age
            {
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
