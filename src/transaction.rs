use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use arrow::record_batch::RecordBatch;

use crate::data_store::txn_buffer::TxnBuffer;
use crate::data_store::versioned_value::VersionedValue;
use crate::errors::{Result, Error};
use crate::TransactionIsolation;
use crate::conflict::resolution::ConflictResolution;
use crate::conflict::detection::{detect_conflicts, ConflictType}; // Import the conflict detection function and ConflictType
use crate::storage::{Storage, StorageMutation}; // Import the Storage trait and StorageMutation

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
        }
    }

    /// Reads data associated with a key.
    pub fn read(&mut self, key: &String) -> Result<Option<Arc<RecordBatch>>> {
        // TODO: Implement read logic based on isolation level and read/write sets.
        // For now, a simple read from the transaction buffer, considering the write set.

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
        self.write_set.insert(key, Some(record_batch));
        Ok(())
    }

    /// Stages a delete operation for a key.
    pub fn delete(&mut self, key: &str) -> Result<()> {
        // Stage the deletion in the write set.
        self.write_set.insert(key.to_string(), None);
        Ok(())
    }

    /// Attempts to commit the transaction.
    pub fn commit(mut self) -> Result<()> { // Added mut to self
        // Phase 1: Validation and Conflict Detection
        // Acquire a commit timestamp. Note: This should ideally be done *after* validation
        // in optimistic concurrency control, but for basic structure, we'll get it here.
        let commit_timestamp = self.transaction_counter.fetch_add(1, Ordering::SeqCst);

        // Perform conflict detection
        let conflicts = detect_conflicts(
            self.id,
            self.isolation_level,
            &self.read_set,
            &self.write_set, // Pass the actual write set
            &self.txn_buffer,
        )?;

        let write_set_to_apply = &mut self.write_set; // Work with a mutable reference

        if !conflicts.is_empty() {
            match self.conflict_resolution {
                ConflictResolution::Fail => {
                    // Fail the transaction on conflict
                    return Err(Error::TransactionConflict);
                }
                ConflictResolution::Ignore => {
                    // Ignore resolution: Filter out conflicting changes from the write_set.
                    write_set_to_apply.retain(|key, _| !conflicts.contains_key(key)); // Filter in place
                    println!("Conflict detected for transaction {}. Resolution: Ignore. Filtered {} conflicting changes.", self.id, conflicts.len());
                }
                ConflictResolution::Replace => {
                    // TODO: Implement Replace resolution:
                    // If a conflict is detected, the transaction's changes should overwrite
                    // the conflicting data in the txn_buffer. The current txn_buffer.insert
                    // already replaces, but ensuring this is atomic for the entire set of
                    // conflicting keys might require more sophisticated logic or reliance
                    // on the underlying SkipMap's atomicity per key.
                    println!("Conflict detected for transaction {}. Resolution: Replace. TODO: Ensure atomic replacement of conflicting data.", self.id);
                    // For now, the current logic proceeds with applying changes using insert, which replaces,
                    // but the atomicity guarantee for the entire transaction's changes is limited.
                }
                ConflictResolution::Append => {
                    // TODO: Implement Append resolution:
                    // If a conflict is detected, for the conflicting keys, the new data
                    // from the write_set needs to be merged with the existing data in the txn_buffer.
                    // This requires:
                    // 1. Identifying conflicting keys (part of conflict detection).
                    // 2. For each conflicting key, retrieve the current data from the txn_buffer.
                    // 3. Retrieve the new data from the write_set.
                    // 4. Use an Arrow utility function (e.g., `merge_record_batches`) to merge the current data and the new data.
                    // 5. Update the write_set with the merged RecordBatch for this key.
                    // 6. After processing all conflicting keys, proceed with applying the updated write_set.
                    println!("Conflict detected for transaction {}. Resolution: Append. TODO: Implement data merging using Arrow utility functions.", self.id);
                    // For now, the current logic proceeds with applying changes, which is incorrect for Append.
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
        // This is a simplification and might not guarantee atomicity of the *entire*
        // transaction's changes in the face of concurrent commits without further mechanisms.

        let mut mutations_to_persist: Vec<StorageMutation> = Vec::new();

        for (key, change) in write_set_to_apply.drain() { // Iterate over the mutable reference and drain
            match change {
                Some(record_batch) => {
                    // Insert or update the value in the transaction buffer
                    let versioned_value = VersionedValue::new(Arc::new(record_batch.clone()), commit_timestamp);
                    self.txn_buffer.insert(key.clone(), versioned_value);
                    // Memory reclamation for the old value is handled within txn_buffer.insert
                    mutations_to_persist.push(StorageMutation::Insert(key, record_batch));
                }
                None => {
                    // Delete the value from the transaction buffer
                    self.txn_buffer.delete(&key.clone()); // Explicitly clone and pass &String
                    // Memory reclamation for the deleted value is handled within txn_buffer.delete
                    mutations_to_persist.push(StorageMutation::Delete(key));
                }
            }
        }


        // Call the Storage trait to persist changes.
        // This should happen after the in-memory state is updated.
        self.storage.apply_mutations(mutations_to_persist)?;


        Ok(()) // Placeholder for successful commit
    }

    /// Aborts the transaction, discarding staged changes.
    pub fn rollback(self) {
        // TODO: Implement rollback logic (simply dropping the struct is a good start).
        println!("Transaction {} rolled back", self.id);
    }
}
