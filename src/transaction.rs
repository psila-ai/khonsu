use ahash::{AHashMap as HashMap, AHashSet as HashSet}; // Keep HashSet import
use arrow::record_batch::RecordBatch;
use log::debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::conflict::detection::detect_conflicts;
use crate::conflict::resolution::ConflictResolution;
use crate::data_store::txn_buffer::TxnBuffer;
use crate::data_store::versioned_value::VersionedValue;
use crate::dependency_tracking::{DataItem, DependencyTracker}; // Keep DependencyTracker
use crate::errors::{KhonsuError, Result};
use crate::storage::{Storage, StorageMutation};
use crate::TransactionIsolation;

/// Represents a single transaction.
pub struct Transaction {
    /// Unique identifier for the transaction (timestamp).
    id: u64,
    /// The isolation level for this transaction.
    isolation_level: TransactionIsolation,
    /// Reference to the transaction buffer.
    txn_buffer: Arc<TxnBuffer>,
    /// Reference to the global transaction counter.
    transaction_counter: Arc<AtomicU64>,
    /// Reference to the storage implementation.
    storage: Arc<dyn Storage>,
    /// The set of data items read by this transaction and their versions.
    read_set: HashMap<String, u64>,
    /// Staged changes (insertions, updates, deletions) for this transaction.
    // Using Option<RecordBatch> to represent insertions/updates (Some) and deletions (None).
    write_set: HashMap<String, Option<RecordBatch>>,
    /// The conflict resolution strategy for this transaction.
    conflict_resolution: ConflictResolution,
    /// Reference to the dependency tracker for serializability.
    dependency_tracker: Arc<DependencyTracker>,
}

impl Transaction {
    /// Returns the unique identifier of the transaction.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Creates a new transaction.
    pub fn new(
        id: u64,
        isolation_level: TransactionIsolation,
        txn_buffer: Arc<TxnBuffer>,
        transaction_counter: Arc<AtomicU64>,
        storage: Arc<dyn Storage>,
        conflict_resolution: ConflictResolution,
        dependency_tracker: Arc<DependencyTracker>,
    ) -> Self {
        // Register the transaction with the tracker when it starts
        dependency_tracker.register_txn(id);
        Self {
            id,
            isolation_level,
            txn_buffer,
            transaction_counter,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            conflict_resolution,
            dependency_tracker,
        }
    }

    /// Reads data associated with a key.
    pub fn read(&mut self, key: &String) -> Result<Option<Arc<RecordBatch>>> {
        // Check the write set first
        if let Some(change) = self.write_set.get(key) {
            return Ok(change.as_ref().map(|rb| Arc::new(rb.clone())));
        }

        // Read from the transaction buffer
        let versioned_value = self.txn_buffer.get(key);

        match self.isolation_level {
            TransactionIsolation::ReadCommitted => {
                if let Some(value) = versioned_value {
                    // Don't record reads for ReadCommitted in read_set for OCC
                    Ok(Some(value.data().clone()))
                } else {
                    Ok(None)
                }
            }
            TransactionIsolation::RepeatableRead | TransactionIsolation::Serializable => {
                if let Some(value) = versioned_value {
                    let version = value.version();
                    self.read_set.insert(key.to_string(), version);
                    // For Serializable, record read in dependency tracker
                    if self.isolation_level == TransactionIsolation::Serializable {
                        let data_item = DataItem { key: key.clone() };
                        self.dependency_tracker
                            .record_read(self.id, data_item, version);
                    }
                    Ok(Some(value.data().clone()))
                } else {
                    // Record the fact that the key didn't exist at this point (version 0)
                    self.read_set.insert(key.to_string(), 0);
                    Ok(None)
                }
            }
        }
    }

    /// Stages a write operation for a key with the provided RecordBatch.
    pub fn write(&mut self, key: String, record_batch: RecordBatch) -> Result<()> {
        // Record write intention immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem { key: key.clone() }; // Clone key for data_item
            self.dependency_tracker
                .record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        // Stage the write/update in the write set.
        self.write_set.insert(key, Some(record_batch));
        Ok(())
    }

    /// Stages a delete operation for a key.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        let key_string = key.to_string();
        // Record write intention immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem {
                key: key_string.clone(),
            }; // Clone key for data_item
            self.dependency_tracker
                .record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        // Stage the deletion in the write set.
        self.write_set.insert(key_string, None);
        Ok(())
    }

    /// Attempts to commit the transaction.
    pub fn commit(self) -> Result<()> {
        // Phase 1: Validation and Conflict Detection
        let commit_timestamp = self.transaction_counter.fetch_add(1, Ordering::SeqCst);

        // Take ownership of write_set early
        let mut write_set_to_apply = self.write_set;
        // Need the keys for validation/marking committed
        let write_set_keys: HashSet<String> = write_set_to_apply.keys().cloned().collect();

        // Perform validation based on isolation level
        let conflicts = match self.isolation_level {
            TransactionIsolation::Serializable => {
                // For Serializable, perform SSI validation ONLY.
                // let write_set_keys: HashSet<String> = write_set_to_apply.keys().cloned().collect(); // Moved up
                if !self.dependency_tracker.validate_serializability(
                    self.id,
                    commit_timestamp, // Pass commit_ts
                    &self.read_set,
                    &write_set_keys,  // Use cloned keys
                    &self.txn_buffer, // Pass buffer
                )? {
                    // SSI validation failed (backward or forward check).
                    self.dependency_tracker.mark_aborted(self.id); // Mark as aborted in tracker
                    return Err(KhonsuError::TransactionConflict);
                }
                // If SSI validation passes, assume no conflicts for this level.
                HashMap::new() // Return empty conflicts
            }
            TransactionIsolation::ReadCommitted | TransactionIsolation::RepeatableRead => {
                // For other levels, perform standard OCC conflict detection.
                detect_conflicts(
                    self.id, // Pass start_ts (which is id)
                    self.isolation_level,
                    &self.read_set,
                    &write_set_to_apply, // Pass original write_set map
                    &self.txn_buffer,
                    // &self.dependency_tracker, // Removed tracker argument
                )?
            }
        };

        // If OCC checks (for non-Serializable) found conflicts:
        if !conflicts.is_empty() {
            // Standard conflict resolution logic
            match self.conflict_resolution {
                ConflictResolution::Fail => {
                    // Mark as aborted (tracker handles non-serializable cases gracefully)
                    self.dependency_tracker.mark_aborted(self.id);
                    return Err(KhonsuError::TransactionConflict);
                }
                ConflictResolution::Ignore => {
                    write_set_to_apply.retain(|key, _| !conflicts.contains_key(key));
                    debug!("Conflict detected for transaction {}. Resolution: Ignore. Filtered {} conflicting changes.", self.id, conflicts.len());
                }
                ConflictResolution::Replace => {
                    debug!("Conflict detected for transaction {}. Resolution: Replace. Conflicting changes will overwrite existing data.", self.id);
                }
                ConflictResolution::Append => {
                    debug!("Conflict detected for transaction {}. Resolution: Append. Merging conflicting changes.", self.id);
                    let mut merged_changes: HashMap<String, Option<RecordBatch>> = HashMap::new();
                    for (key, _conflict_type) in &conflicts {
                        if let Some(change) = write_set_to_apply.get(key) {
                            if let Some(new_data) = change {
                                let current_data = self.txn_buffer.get(key);
                                if let Some(existing_value) = current_data {
                                    let merged_record_batch =
                                        crate::arrow_utils::merge_record_batches(
                                            existing_value.data(),
                                            &HashMap::from([(key.clone(), Some(new_data.clone()))]),
                                            "id",
                                        )?;
                                    merged_changes.insert(key.clone(), Some(merged_record_batch));
                                } else {
                                    merged_changes.insert(key.clone(), Some(new_data.clone()));
                                }
                            } else {
                                debug!("Conflict on deletion for key {}. Append resolution not applicable.", key);
                                merged_changes.insert(key.clone(), None);
                            }
                        } else {
                            debug!("Conflict on read-only key {}. No Append resolution action needed on write set.", key);
                        }
                    }
                    for (key, merged_change) in merged_changes {
                        write_set_to_apply.insert(key, merged_change);
                    }
                }
            }
        }

        // Phase 2: Apply Changes
        let mut mutations_to_persist: Vec<StorageMutation> = Vec::new();
        for (key, change) in write_set_to_apply.drain() {
            match change {
                Some(record_batch) => {
                    // Use commit_timestamp as the version for the new value
                    let versioned_value =
                        VersionedValue::new(Arc::new(record_batch.clone()), commit_timestamp);
                    self.txn_buffer.insert(key.clone(), versioned_value);
                    mutations_to_persist.push(StorageMutation::Insert(key, record_batch));
                }
                None => {
                    self.txn_buffer.delete(&key.clone());
                    mutations_to_persist.push(StorageMutation::Delete(key));
                }
            }
        }

        self.storage.apply_mutations(mutations_to_persist)?;

        // Mark as committed in tracker
        // Pass the cloned write_set_keys
        self.dependency_tracker
            .mark_committed(self.id, commit_timestamp, write_set_keys);

        Ok(())
    }

    /// Aborts the transaction, discarding staged changes.
    pub fn rollback(self) {
        debug!("Transaction {} rolled back", self.id);
        // Mark as aborted in tracker
        self.dependency_tracker.mark_aborted(self.id);
        // The `write_set` and `read_set` are dropped when `self` is dropped.
    }
}
