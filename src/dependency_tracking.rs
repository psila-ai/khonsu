use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use log::debug;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering}; // Needed for cleanup counter
use std::time::{Duration, Instant}; // Needed for cleanup timing

use crate::data_store::txn_buffer::TxnBuffer; // Keep this import
use crate::errors::*;

/// Represents the state of a transaction being tracked within the `DependencyTracker`.
///
/// This enum is used to indicate the current lifecycle stage of a transaction,
/// which is crucial for correctly applying Serializable Snapshot Isolation (SSI)
/// validation rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnState {
    /// The transaction is currently active and has not yet attempted to commit or abort.
    Active,
    /// The transaction has successfully committed.
    Committed,
    /// The transaction has been aborted.
    Aborted,
}

/// Holds information about a transaction relevant for SSI validation.
///
/// The `DependencyTracker` stores instances of this struct to keep track of
/// the state and relevant metadata for each transaction, enabling the detection
/// of serialization anomalies.
#[derive(Debug, Clone)]
pub(crate) struct TransactionInfo {
    /// The current state of the transaction (`Active`, `Committed`, or `Aborted`).
    pub(crate) state: TxnState,
    /// The timestamp when the transaction started (typically its unique transaction ID).
    pub(crate) start_ts: u64, // Transaction ID acts as start timestamp
    /// The timestamp when the transaction committed, if applicable.
    pub(crate) commit_ts: Option<u64>,
    /// The set of keys that this transaction wrote to upon successful commit.
    /// This is used for forward validation against committed transactions.
    pub(crate) write_set_keys: Option<HashSet<String>>,
    /// The time when this `TransactionInfo` was created, used for garbage collection.
    pub(crate) creation_time: Instant, // For cleanup purposes
}

/// Represents a data item for dependency tracking purposes.
///
/// This struct identifies a specific piece of data within the transaction
/// buffer that transactions might read from or write to. It is used by the
/// `DependencyTracker` to record dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataItem {
    /// The unique key identifying the data item.
    pub key: String,
    // Potentially other relevant data item information (e.g., version)
}

/// Tracks readers and writers for a specific data item, including versions.
///
/// This struct is stored within the `item_dependencies` map of the
/// `DependencyTracker` and records which active transactions have read
/// or are intending to write to the associated data item.
#[derive(Debug, Clone, Default)]
pub(crate) struct ItemDependency {
    /// Map from *active* reader transaction ID to the version they read.
    /// This is used for backward validation in SSI.
    pub(crate) readers: HashMap<u64, u64>,
    /// Set of *active* writer transaction IDs.
    /// This is used for forward validation in SSI.
    pub(crate) writers: HashSet<u64>,
}

/// A concurrent data structure to track transaction dependencies for SSI validation.
///
/// The `DependencyTracker` is responsible for maintaining information about
/// active, committed, and aborted transactions, as well as tracking read and
/// write dependencies between active transactions and data items. This information
/// is used to perform Serializable Snapshot Isolation (SSI) validation during
/// transaction commit.
pub struct DependencyTracker {
    /// Tracks state (`Active`, `Committed`, `Aborted`) and timestamps for transactions.
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

impl Default for DependencyTracker {
    /// Creates a new `DependencyTracker` with default configuration.
    fn default() -> Self {
        Self::new()
    }
}

impl DependencyTracker {
    /// Creates a new `DependencyTracker`.
    ///
    /// Initializes the internal data structures and configuration for tracking
    /// transaction dependencies and performing SSI validation.
    ///
    /// # Returns
    ///
    /// A new `DependencyTracker` instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    ///
    /// let dependency_tracker = DependencyTracker::new();
    /// ```
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
    ///
    /// This method is called when a transaction starts. It adds the transaction
    /// to the tracker's internal state with the `Active` status and records
    /// its start timestamp.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The unique identifier of the transaction to register.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    ///
    /// let dependency_tracker = Arc::new(DependencyTracker::new());
    /// let transaction_id = 1; // Example transaction ID
    /// dependency_tracker.register_txn(transaction_id);
    /// ```
    pub fn register_txn(&self, txn_id: u64) {
        let info = TransactionInfo {
            state: TxnState::Active,
            start_ts: txn_id,
            commit_ts: None,
            write_set_keys: None, // Write set only added on commit
            creation_time: Instant::now(),
        };
        self.transactions.write().insert(txn_id, info);
        debug!("Registered Tx {} as Active", txn_id);
        self.maybe_trigger_cleanup(); // Periodically cleanup
    }

    /// Marks a transaction as Committed. Stores write set keys for recent commits.
    ///
    /// This method is called when a transaction successfully commits. It updates
    /// the transaction's state to `Committed`, records its commit timestamp,
    /// and stores the set of keys it wrote to. It also removes the transaction's
    /// active dependencies from the `item_dependencies` map.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The unique identifier of the transaction to mark as committed.
    /// * `commit_ts` - The timestamp assigned to the transaction at commit time.
    /// * `write_set_keys` - The set of keys that the transaction wrote to.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use ahash::AHashSet as HashSet;
    ///
    /// # let dependency_tracker = Arc::new(DependencyTracker::new());
    /// let transaction_id = 1;
    /// let commit_timestamp = 100; // Example commit timestamp
    /// let write_keys: HashSet<String> = vec!["key1".to_string(), "key2".to_string()].into_iter().collect();
    ///
    /// // Assuming the transaction was previously registered as active.
    /// // dependency_tracker.register_txn(transaction_id);
    ///
    /// dependency_tracker.mark_committed(transaction_id, commit_timestamp, write_keys);
    /// ```
    pub fn mark_committed(&self, txn_id: u64, commit_ts: u64, write_set_keys: HashSet<String>) {
        let found = {
            // Scope the write lock
            let mut transactions_guard = self.transactions.write();
            if let Some(info) = transactions_guard.get_mut(&txn_id) {
                info.state = TxnState::Committed;
                info.commit_ts = Some(commit_ts);
                info.write_set_keys = Some(write_set_keys); // Store write set for recent commits
                debug!(
                    "Marked Tx {} as Committed (CommitTS: {})",
                    txn_id, commit_ts
                );
                true
            } else {
                debug!(
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
    ///
    /// This method is called when a transaction is rolled back or fails to commit.
    /// It updates the transaction's state to `Aborted` and removes its active
    /// dependencies from the `item_dependencies` map.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The unique identifier of the transaction to mark as aborted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # let dependency_tracker = Arc::new(DependencyTracker::new());
    /// let transaction_id = 2; // Example transaction ID
    ///
    /// // Assuming the transaction was previously registered as active.
    /// // dependency_tracker.register_txn(transaction_id);
    ///
    /// dependency_tracker.mark_aborted(transaction_id);
    /// ```
    pub fn mark_aborted(&self, txn_id: u64) {
        let found = {
            // Scope the write lock
            let mut transactions_guard = self.transactions.write();
            if let Some(info) = transactions_guard.get_mut(&txn_id) {
                info.state = TxnState::Aborted;
                info.write_set_keys = None; // Clear write set on abort
                debug!("Marked Tx {} as Aborted", txn_id);
                true
            } else {
                debug!(
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
    ///
    /// This method is called by an active transaction when it reads a data item.
    /// It records the transaction ID and the version of the data item that was read.
    /// This information is used for backward validation (SI Read Check) during SSI.
    ///
    /// # Arguments
    ///
    /// * `reader_id` - The ID of the transaction that performed the read.
    /// * `data_item` - The `DataItem` that was read.
    /// * `read_version` - The version of the data item that was read.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # let dependency_tracker = Arc::new(DependencyTracker::new());
    /// let reader_transaction_id = 3; // Example reader transaction ID
    /// let item = DataItem { key: "some_key".to_string() };
    /// let version_read = 5; // Example version read
    ///
    /// // Assuming the reader transaction is active.
    /// // dependency_tracker.register_txn(reader_transaction_id);
    ///
    /// dependency_tracker.record_read(reader_transaction_id, item, version_read);
    /// ```
    pub fn record_read(&self, reader_id: u64, data_item: DataItem, read_version: u64) {
        if self
            .transactions
            .read()
            .get(&reader_id)
            .is_some_and(|info| info.state == TxnState::Active)
        {
            let mut item_deps = self.item_dependencies.write();
            let item_dep = item_deps.entry(data_item.key.clone()).or_default();
            item_dep.readers.insert(reader_id, read_version);
            debug!(
                "Recorded active read: Tx {} read {:?} at version {}",
                reader_id, data_item, read_version
            );
        }
    }

    /// Records that an *active* transaction intends to write to a data item.
    ///
    /// This method is called by an active transaction when it stages a write
    /// operation for a data item. It records the transaction ID as an active
    /// writer for the specified data item. This information is used for forward
    /// validation (RW-Dependency Check) during SSI.
    ///
    /// # Arguments
    ///
    /// * `writer_id` - The ID of the transaction that intends to write.
    /// * `data_item` - The `DataItem` that the transaction intends to write to.
    /// * `_write_version` - The intended version of the write (currently unused).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    ///
    /// # let dependency_tracker = Arc::new(DependencyTracker::new());
    /// let writer_transaction_id = 4; // Example writer transaction ID
    /// let item = DataItem { key: "another_key".to_string() };
    ///
    /// // Assuming the writer transaction is active.
    /// // dependency_tracker.register_txn(writer_transaction_id);
    ///
    /// dependency_tracker.record_write(writer_transaction_id, item, 0); // Version is currently unused
    /// ```
    pub fn record_write(&self, writer_id: u64, data_item: DataItem, _write_version: u64) {
        if self
            .transactions
            .read()
            .get(&writer_id)
            .is_some_and(|info| info.state == TxnState::Active)
        {
            let mut item_deps = self.item_dependencies.write();
            let item_dep = item_deps.entry(data_item.key.clone()).or_default();
            item_dep.writers.insert(writer_id); // Just track the active writer ID
            debug!(
                "Recorded active write intention: Tx {} intends to write to {:?}",
                writer_id, data_item
            );
        }
    }

    /// Validates serializability using SSI principles.
    ///
    /// This is the core SSI validation logic. It performs two main checks:
    /// 1.  **Backward Validation (SI Read Check):** Ensures that no data item
    ///     read by the committing transaction was overwritten by another transaction
    ///     that committed *after* the committing transaction started.
    /// 2.  **Forward Validation (RW-Dependency Check):** Detects dangerous read-write
    ///     dependency cycles involving the committing transaction and other
    ///     concurrent (active or recently committed) transactions. Specifically,
    ///     it checks for the existence of both an incoming RW edge (T_concurrent
    ///     writes x, T_commit reads x) and an outgoing RW edge (T_commit writes y,
    ///     T_active reads y).
    ///
    /// If either of these checks fails, the transaction cannot be committed
    /// under Serializable isolation.
    ///
    /// # Arguments
    ///
    /// * `committing_tx_id` - The ID of the transaction attempting to commit.
    /// * `_commit_ts` - The timestamp assigned to the transaction at commit time
    ///   (currently not strictly needed for the basic SI backward check).
    /// * `read_set` - The read set of the committing transaction (keys and versions read).
    /// * `write_set` - The write set of the committing transaction (keys written).
    /// * `txn_buffer` - A reference to the shared `TxnBuffer` for checking current versions.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the transaction passes SSI validation and can commit.
    /// Returns `Ok(false)` if the transaction fails SSI validation due to a
    /// serialization anomaly.
    /// Returns `Err(KhonsuError)` if an error occurs during validation.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError` if an internal error occurs during the validation process.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use ahash::{AHashMap as HashMap, AHashSet as HashSet};
    ///
    /// # let dependency_tracker = Arc::new(DependencyTracker::new());
    /// # let txn_buffer = Arc::new(TxnBuffer::new());
    /// let committing_transaction_id = 5; // Example transaction ID
    /// let commit_timestamp = 200; // Example commit timestamp
    /// let read_keys: HashMap<String, u64> = vec![("key_a".to_string(), 10)].into_iter().collect(); // Example read set
    /// let write_keys: HashSet<String> = vec!["key_b".to_string()].into_iter().collect(); // Example write set
    ///
    /// // Assuming the transaction is active and dependencies have been recorded.
    /// // dependency_tracker.register_txn(committing_transaction_id);
    /// // dependency_tracker.record_read(...);
    /// // dependency_tracker.record_write(...);
    ///
    /// match dependency_tracker.validate_serializability(
    ///     committing_transaction_id,
    ///     commit_timestamp,
    ///     &read_keys,
    ///     &write_keys,
    ///     &txn_buffer,
    /// ) {
    ///     Ok(true) => {
    ///         println!("Transaction {} passed SSI validation.", committing_transaction_id);
    ///     }
    ///     Ok(false) => {
    ///         eprintln!("Transaction {} failed SSI validation.", committing_transaction_id);
    ///     }
    ///     Err(e) => {
    ///         eprintln!("Error during SSI validation: {}", e);
    ///     }
    /// }
    /// ```
    pub fn validate_serializability(
        &self,
        committing_tx_id: u64,
        _commit_ts: u64, // commit_ts is not strictly needed for basic SI backward check
        read_set: &HashMap<String, u64>, // Read set of the committing transaction
        write_set: &HashSet<String>, // Write set of the committing transaction
        txn_buffer: &TxnBuffer, // Need buffer for backward validation
    ) -> Result<bool> {
        debug!("Starting SSI validation for Tx {}", committing_tx_id);
        let transactions_read_lock = self.transactions.read();

        let start_ts = match transactions_read_lock.get(&committing_tx_id) {
            Some(info) if info.state == TxnState::Active => info.start_ts,
            _ => {
                debug!(
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
                        debug!(
                            "SSI Backward Validation Failed (RW): Tx {} read key '{}' at ver {}, but Tx {} committed ver {} at TS {}",
                            committing_tx_id,
                            key,
                            read_version,
                            writer_commit_ts,
                            writer_commit_ts,
                            writer_commit_ts
                        );
                        return Ok(false);
                    }
                }
            } else {
                // Item we read was deleted. Conflict if deletion committed after we started.
                if *read_version > 0 {
                    // Heuristic: If we read version > 0, it existed
                    debug!(
                        "SSI Backward Validation Failed (RD Heuristic): Tx {} read key '{}' at ver {}, but it was deleted.",
                        committing_tx_id, key, read_version
                    );
                    return Ok(false);
                }
            }
        }

        // 2. WW Check: Check if any item written by T_commit was also written by a transaction T_other
        //    that committed *after* T_commit started.
        for key in write_set {
            if let Some(current_value) = txn_buffer.get(key) {
                let writer_commit_ts = current_value.version();
                debug!(
                    "  SSI WW Check: Key='{}', CommittingTx={}, StartTs={}, BufferVersion={}",
                    key, committing_tx_id, start_ts, writer_commit_ts
                ); // ADDED LOG
                // Conflict if writer committed after we started.
                if writer_commit_ts > start_ts {
                    debug!(
                        "  SSI Backward Validation Failed (WW): Tx {} writing key '{}', but Tx {} committed ver {} at TS {}",
                        committing_tx_id, key, writer_commit_ts, writer_commit_ts, writer_commit_ts
                    );
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
                    if *reader_id != committing_tx_id
                        && transactions_read_lock
                            .get(reader_id)
                            .is_some_and(|info| info.state == TxnState::Active)
                    {
                        debug!(
                            "SSI Forward Validation: Found outgoing RW edge from Tx {} to active Tx {} on key '{}'",
                            committing_tx_id, reader_id, write_key
                        );
                        has_outgoing_rw_edge = true;
                        if has_incoming_rw_edge {
                            break;
                        } // Optimization
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
                        if *writer_id != committing_tx_id
                            && transactions_read_lock
                                .get(writer_id)
                                .is_some_and(|info| info.state == TxnState::Active)
                        {
                            debug!(
                                "SSI Forward Validation: Found incoming RW edge from active Tx {} to Tx {} on key '{}'",
                                writer_id, committing_tx_id, read_key
                            );
                            has_incoming_rw_edge = true;
                            if has_outgoing_rw_edge {
                                break;
                            } // Optimization
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
                                debug!(
                                    "SSI Forward Validation: Found incoming RW edge from committed Tx {} to Tx {} on key '{}'",
                                    other_txn_id, committing_tx_id, read_key
                                );
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
            debug!(
                "SSI Forward Validation Failed: Dangerous RW structure detected for Tx {}",
                committing_tx_id
            );
            return Ok(false);
        }

        debug!("SSI validation successful for Tx {}", committing_tx_id);
        Ok(true)
    }

    /// Removes a transaction's read/write entries from the item_dependencies map.
    ///
    /// This private helper method is called after a transaction commits or aborts
    /// to clean up its entries in the `item_dependencies` map. This is important
    /// for removing dependencies involving inactive transactions.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction whose dependencies should be removed.
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
        debug!("Removed active dependencies for transaction {}", tx_id);
    }

    /// Periodically checks and removes old transaction info.
    ///
    /// This private helper method is called after certain operations to
    /// probabilistically trigger a cleanup of old transaction information
    /// from the `transactions` map.
    fn maybe_trigger_cleanup(&self) {
        let count = self.cleanup_counter.fetch_add(1, Ordering::Relaxed);
        if count >= self.cleanup_threshold {
            self.cleanup_counter.store(0, Ordering::Relaxed); // Reset counter
            self.cleanup_old_txns();
        }
    }

    /// Removes info for transactions older than max_txn_age.
    ///
    /// This private helper method performs the actual garbage collection of
    /// old transaction information from the `transactions` map. It retains
    /// active transactions and recently committed/aborted ones based on
    /// the configured `max_txn_age`.
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
            debug!("Cleaned up info for {} old transactions.", removed_count);
        }
    }
}
