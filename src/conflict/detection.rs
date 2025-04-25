use std::collections::HashMap;
use arrow::record_batch::RecordBatch;

use crate::data_store::txn_buffer::TxnBuffer;
use crate::errors::{Result, Error};
use crate::TransactionIsolation;

/// Represents the type of conflict detected.
#[derive(Debug, PartialEq, Eq)]
pub enum ConflictType {
    /// The transaction read data that was modified by another transaction.
    ReadWrite,
    /// The transaction attempted to write to data that was modified by another transaction.
    WriteWrite,
    /// The transaction read data that was deleted by another transaction.
    ReadDelete,
    // TODO: Add other conflict types if necessary (e.g., WriteDelete).
}

/// Checks for conflicts between a transaction's read/write sets and the transaction buffer.
///
/// This function implements optimistic concurrency control validation and returns
/// a map of conflicting keys and the type of conflict.
pub fn detect_conflicts(
    transaction_id: u64, // Transaction ID might be useful for logging/debugging
    isolation_level: TransactionIsolation,
    read_set: &HashMap<String, u64>,
    write_set: &HashMap<String, Option<RecordBatch>>,
    txn_buffer: &TxnBuffer,
) -> Result<HashMap<String, ConflictType>> {
    let mut conflicts: HashMap<String, ConflictType> = HashMap::new();

    match isolation_level {
        TransactionIsolation::ReadCommitted => {
            // ReadCommitted: No read validation needed for reads.
            // Write-write conflicts are implicitly handled by the underlying TxnBuffer (SkipMap),
            // typically resulting in last-writer-wins for a specific key.
            // If different write-write conflict resolution strategies are needed (e.g., Append),
            // more explicit checks would be required here or during the apply phase.
            // For detection purposes at this level, we might check for write-write conflicts
            // if the resolution strategy requires explicit handling before applying.
            // TODO: Add explicit Write-Write conflict detection if needed for specific resolution strategies.
        }
        TransactionIsolation::RepeatableRead | TransactionIsolation::Serializable => {
            // RepeatableRead/Serializable: Validate the read set.
            // Check if any data read by this transaction has been modified (committed by another transaction)
            // since it was read.
            for (key, read_version) in read_set {
                if let Some(current_value) = txn_buffer.get(key) {
                    if current_value.version() > *read_version {
                        // Data read by this transaction has been modified by another transaction.
                        conflicts.insert(key.clone(), ConflictType::ReadWrite);
                    }
                } else {
                    // Data read by this transaction was deleted by another transaction.
                    // This is also a conflict for RepeatableRead/Serializable.
                    conflicts.insert(key.clone(), ConflictType::ReadDelete);
                }
            }

            // Check for Write-Write Conflicts:
            // If this transaction is trying to write to a key that was modified by another
            // transaction concurrently.
            // Iterate through the write set and check the current version in the txn_buffer.
            // If the current version is newer than the version the transaction read for that key,
            // or newer than the transaction's ID if the key was not read, it's a write-write conflict.
            for (key, _change) in write_set {
                 if let Some(current_value) = txn_buffer.get(key) {
                    // Get the version of the key as seen by this transaction.
                    // If the key was read, use the version from the read set.
                    // If the key was not read, use the transaction's ID as a proxy for its start version.
                    let transaction_version_of_key = read_set.get(key).copied().unwrap_or(transaction_id);

                    if current_value.version() > transaction_version_of_key {
                         // Data being written by this transaction was modified by another transaction
                         // after this transaction read it (or started, if not read).
                         conflicts.insert(key.clone(), ConflictType::WriteWrite);
                    }
                 } else {
                     // The key exists in the write set but not in the txn_buffer.
                     // This could happen if the key was inserted by this transaction,
                     // or if it was deleted by another transaction concurrently.
                     // If it was deleted by another transaction, it's a Write-Delete conflict.
                     // TODO: Add Write-Delete conflict type and detection if needed.
                 }
            }


            if isolation_level == TransactionIsolation::Serializable {
                // Serializable: Additional checks for serializability anomalies.
                // This includes checking for Write-Read conflicts:
                // If another transaction read data that this transaction is writing,
                // and that other transaction committed after this one started.
                // This requires tracking read sets of other active/recently committed transactions,
                // which is complex and not directly supported by the current TxnBuffer structure.
                // A full Serializable implementation typically involves a validation phase
                // that checks for cycles in a dependency graph or uses a global lock/timestamp
                // for commit ordering.
                // TODO: Implement full Serializable validation, including Write-Read conflicts.
                println!("TODO: Implement full Serializable validation checks, including Write-Read conflicts.");
            }
        }
    }

    Ok(conflicts)
}
