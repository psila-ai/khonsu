use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::conflict::detection::detect_conflicts;
use crate::conflict::resolution::ConflictResolution;
use crate::data_store::txn_buffer::TxnBuffer;
use crate::data_store::versioned_value::VersionedValue;
use crate::dependency_tracking::{DataItem, DependencyTracker};
use crate::errors::{Error, Result};
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
    // The actual creation will be handled by the Khonsu struct, which provides the ID and buffer reference.
    pub fn new(
        id: u64,
        isolation_level: TransactionIsolation,
        txn_buffer: Arc<TxnBuffer>,
        transaction_counter: Arc<AtomicU64>,
        storage: Arc<dyn Storage>,
        conflict_resolution: ConflictResolution,
        dependency_tracker: Arc<DependencyTracker>, // Add dependency_tracker parameter
    ) -> Self {
        Self {
            id,
            isolation_level,
            txn_buffer,
            transaction_counter,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            conflict_resolution,
            dependency_tracker, // Initialize dependency_tracker field
        }
    }

    /// Reads data associated with a key.
    pub fn read(&mut self, key: &String) -> Result<Option<Arc<RecordBatch>>> {
        // Reads data based on isolation level and read/write sets.

        // Check the write set first for any staged changes (including deletions)
        if let Some(change) = self.write_set.get(key) {
            match change {
                Some(record_batch) => return Ok(Some(Arc::new(record_batch.clone()))), // Staged write/update
                None => return Ok(None), // Staged deletion
            }
        }

        // If not in the write set, read from the transaction buffer
        let versioned_value = self.txn_buffer.get(key);

        match self.isolation_level {
            TransactionIsolation::ReadCommitted => {
                // ReadCommitted: Always read the latest committed value.
                if let Some(value) = versioned_value {
                    // Record the read in the read set for potential future use (e.g., Serializable validation)
                    self.read_set.insert(key.to_string(), value.version());
                    Ok(Some(value.data().clone()))
                } else {
                    Ok(None)
                }
            }
            TransactionIsolation::RepeatableRead | TransactionIsolation::Serializable => {
                // RepeatableRead/Serializable: Read from the transaction buffer if not in write set. Record the version.
                if let Some(value) = versioned_value {
                    // Record the read and its version
                    self.read_set.insert(key.to_string(), value.version());
                    // For Serializable, record a read operation for dependency tracking.
                    if self.isolation_level == TransactionIsolation::Serializable {
                        let data_item = DataItem { key: key.clone() }; // Create DataItem
                        self.dependency_tracker
                            .record_read(self.id, data_item, value.version());
                    }
                    Ok(Some(value.data().clone()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Stages a write operation for a key with the provided RecordBatch.
    pub fn write(&mut self, key: String, record_batch: RecordBatch) -> Result<()> {
        // Stage the write/update in the write set.
        // For Serializable, dependency tracking for writes will be recorded during commit
        // when the commit timestamp (write version) is available.
        self.write_set.insert(key.clone(), Some(record_batch)); // Clone key for data_item
        // Record write intention immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem { key }; // Use the owned key
            self.dependency_tracker.record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        Ok(())
    }

    /// Stages a delete operation for a key.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        // Stage the deletion in the write set.
        let key_string = key.to_string();
        self.write_set.insert(key_string.clone(), None); // Clone key for data_item
        // Record write intention (deletion is a type of write) immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem { key: key_string }; // Use the owned key
            self.dependency_tracker.record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        Ok(())
    }

    /// Attempts to commit the transaction.
    pub fn commit(mut self) -> Result<()> {
        // Added mut to self
        // Phase 1: Validation and Conflict Detection
        // Acquire a commit timestamp. Note: This should ideally be done *after* validation
        // in optimistic concurrency control, but for basic structure, we'll get it here.
        let commit_timestamp = self.transaction_counter.fetch_add(1, Ordering::SeqCst);

        // // For Serializable isolation, record write dependencies before conflict detection. // MOVED TO WRITE/DELETE
        // if self.isolation_level == TransactionIsolation::Serializable {
        //     // Record write dependencies with the commit timestamp as the write version.
        //     for key in self.write_set.keys() {
        //         let data_item = DataItem { key: key.clone() };
        //         self.dependency_tracker
        //             .record_write(self.id, data_item, commit_timestamp); // Placeholder version was sufficient
        //     }
        // }

        // For Serializable isolation, perform validation (previously cycle detection) *before* standard conflict detection.
        if self.isolation_level == TransactionIsolation::Serializable {
            // Extract keys from write_set for cycle detection
            let write_set_keys: HashSet<String> = self.write_set.keys().cloned().collect();
            if !self.dependency_tracker.check_for_cycles(self.id, &self.read_set, &write_set_keys)? {
                // Cycle detected, serializability violation.
                // Remove dependencies for this transaction as it's aborting.
                self.dependency_tracker.remove_transaction_dependencies(self.id);
                return Err(Error::TransactionConflict);
            }
        }

        // Perform conflict detection, which now includes serializability validation for Serializable transactions.
        let conflicts = detect_conflicts(
            self.id,
            self.isolation_level,
            &self.read_set,
            &self.write_set, // Pass the actual write set
            &self.txn_buffer,
            &self.dependency_tracker, // Pass the dependency tracker
        )?;

        let write_set_to_apply = &mut self.write_set; // Work with a mutable reference

        if !conflicts.is_empty() {
            match self.conflict_resolution {
                ConflictResolution::Fail => {
                    // Fail the transaction on conflict
                    // Remove dependencies for this transaction as it's aborting due to conflict.
                    if self.isolation_level == TransactionIsolation::Serializable {
                        self.dependency_tracker.remove_transaction_dependencies(self.id);
                    }
                    return Err(Error::TransactionConflict);
                }
                ConflictResolution::Ignore => {
                    // Ignore resolution: Filter out conflicting changes from the write_set.
                    write_set_to_apply.retain(|key, _| !conflicts.contains_key(key)); // Filter in place
                    println!("Conflict detected for transaction {}. Resolution: Ignore. Filtered {} conflicting changes.", self.id, conflicts.len());
                }
                ConflictResolution::Replace => {
                    // Replace resolution: If a conflict is detected, the transaction's changes
                    // for the conflicting keys overwrite the existing data in the txn_buffer.
                    // The application of changes in Phase 2 handles this by inserting/deleting
                    // based on the write set, effectively replacing existing entries for the same key.
                    println!("Conflict detected for transaction {}. Resolution: Replace. Conflicting changes will overwrite existing data.", self.id);
                    // No specific action needed here; the logic in Phase 2 applies the replacements.
                }
                ConflictResolution::Append => {
                    // Append resolution: Merge new data with existing data for conflicting keys.
                    println!("Conflict detected for transaction {}. Resolution: Append. Merging conflicting changes.", self.id);

                    // Iterate through the conflicting keys and merge the data.
                    // The merged data will replace the original entry in the write_set_to_apply
                    // before being applied to the txn_buffer in Phase 2.
                    let mut merged_changes: HashMap<String, Option<RecordBatch>> = HashMap::new();

                    for (key, _conflict_type) in &conflicts {
                        // For Append resolution, we primarily care about conflicts on keys
                        // where the transaction has a pending write/update (Some(RecordBatch)).
                        if let Some(change) = write_set_to_apply.get(key) {
                            if let Some(new_data) = change {
                                // Retrieve the current data from the txn_buffer
                                let current_data = self.txn_buffer.get(key);

                                if let Some(existing_value) = current_data {
                                    // Merge existing data with new data
                                    // Assuming a key column named "id" for merging purposes.
                                    // TODO: Make the key column name configurable or part of the schema.
                                    let merged_record_batch =
                                        crate::arrow_utils::merge_record_batches(
                                            existing_value.data(),
                                            &HashMap::from([(key.clone(), Some(new_data.clone()))]), // Create a temporary write set for the single key
                                            "id", // Assuming "id" is the key column name
                                        )?;
                                    merged_changes.insert(key.clone(), Some(merged_record_batch));
                                } else {
                                    // No existing data, this was an insertion that conflicted (e.g., with a delete by another transaction).
                                    // For Append, if the key didn't exist, the insertion should likely still happen.
                                    merged_changes.insert(key.clone(), Some(new_data.clone()));
                                }
                            } else {
                                // Conflict on a deletion. Append resolution on a deletion doesn't make sense.
                                // This case should potentially be an error or ignored depending on semantics.
                                println!("Conflict on deletion for key {}. Append resolution not applicable.", key);
                                // Keep the deletion in the write set.
                                merged_changes.insert(key.clone(), None);
                            }
                        } else {
                            // Conflict on a key that was read but not written by this transaction.
                            // This shouldn't typically trigger an Append resolution action on the write set.
                            println!("Conflict on read-only key {}. No Append resolution action needed on write set.", key);
                        }
                    }

                    // Replace the original entries in write_set_to_apply with the merged changes.
                    for (key, merged_change) in merged_changes {
                        write_set_to_apply.insert(key, merged_change);
                    }
                }
            }
        }

        // Phase 2: Apply Changes
        // Atomically apply changes from the write_set to the txn_buffer.
        // This needs to be atomic for the entire transaction's changes.
        // With SkipMap, individual insert/delete operations are atomic for that key,
        // but the entire set of changes for the transaction is not atomically applied
        // to the *global* state in a single step without additional coordination.
        // A more robust STM would likely use a global version counter and a
        // compare-and-swap loop on a pointer to the root of the data structure,
        // or a log-based approach.
        // For this implementation using SkipMap directly, we iterate and apply.
        // With parking_lot::RwLock, the insert and delete operations on the TxnBuffer
        // are protected by the internal lock. Applying the entire write_set
        // is done by iterating and calling these methods.

        let mut mutations_to_persist: Vec<StorageMutation> = Vec::new();

        for (key, change) in write_set_to_apply.drain() {
            // Iterate over the mutable reference and drain
            match change {
                Some(record_batch) => {
                    // Insert or update the value in the transaction buffer
                    let versioned_value =
                        VersionedValue::new(Arc::new(record_batch.clone()), commit_timestamp);
                    self.txn_buffer.insert(key.clone(), versioned_value); // Use TxnBuffer's insert method
                                                                          // Memory reclamation for the old value is handled within txn_buffer.insert
                    mutations_to_persist.push(StorageMutation::Insert(key, record_batch));
                }
                None => {
                    // Delete the value from the transaction buffer
                    self.txn_buffer.delete(&key.clone()); // Use TxnBuffer's delete method
                                                          // Memory reclamation for the deleted value is handled within txn_buffer.delete
                    mutations_to_persist.push(StorageMutation::Delete(key));
                }
            }
        }

        // Call the Storage trait to persist changes.
        // This should happen after the in-memory state is updated.
        self.storage.apply_mutations(mutations_to_persist)?;

        // Remove dependencies for this transaction after successful commit.
        if self.isolation_level == TransactionIsolation::Serializable {
            self.dependency_tracker.remove_transaction_dependencies(self.id);
        }

        Ok(()) // Successful commit
    }

    /// Aborts the transaction, discarding staged changes.
    /// Dropping the Transaction struct automatically discards the staged changes
    /// held in the `write_set`.
    pub fn rollback(self) {
        println!("Transaction {} rolled back", self.id);
        // The `write_set` and `read_set` are dropped when `self` is dropped.
    }
}
