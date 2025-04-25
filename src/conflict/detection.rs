use ahash::AHashMap as HashMap;
use arrow::record_batch::RecordBatch;
// use std::sync::Arc; // No longer needed

use crate::data_store::txn_buffer::TxnBuffer;
// use crate::dependency_tracking::DependencyTracker; // No longer needed
use crate::errors::*;
use crate::TransactionIsolation;

/// Represents the type of conflict detected (against committed versions).
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ConflictType {
    /// The transaction read data that was modified by another committed transaction.
    CommittedReadWrite,
    /// The transaction attempted to write to data that was modified by another committed transaction.
    CommittedWriteWrite,
    /// The transaction read data that was deleted by another committed transaction.
    CommittedReadDelete,
    /// The transaction attempted to write to data that was deleted by another committed transaction.
    CommittedWriteDelete, // Keep for potential future use
}

/// Checks for conflicts between a transaction's read/write sets and the transaction buffer.
/// Implements OCC validation against committed versions for ReadCommitted and RepeatableRead.
pub fn detect_conflicts(
    committing_tx_id: u64, // Represents the transaction's logical start time for OCC
    isolation_level: TransactionIsolation,
    read_set: &HashMap<String, u64>,
    write_set: &HashMap<String, Option<RecordBatch>>,
    txn_buffer: &TxnBuffer,
    // _dependency_tracker: &Arc<DependencyTracker>, // Parameter no longer needed
) -> Result<HashMap<String, ConflictType>> {
    let mut conflicts: HashMap<String, ConflictType> = HashMap::new();

    // --- OCC Checks against Committed Versions ---

    match isolation_level {
        TransactionIsolation::ReadCommitted => {
            // ReadCommitted: Only check for WW conflicts against committed versions.
            for (key, _change) in write_set {
                if let Some(current_value) = txn_buffer.get(key) {
                    // Conflict if another transaction committed a change after this one started.
                    if current_value.version() > committing_tx_id {
                        println!("Conflict Detail: Committed WW (RC) on '{}' (Tx {} start {}, Buffer ver {})", key, committing_tx_id, committing_tx_id, current_value.version());
                        conflicts.insert(key.clone(), ConflictType::CommittedWriteWrite);
                    }
                }
            }
        }
        TransactionIsolation::RepeatableRead => {
            // RepeatableRead: Check both reads and writes against committed versions.

            // Read Validation
            for (key, read_version) in read_set {
                if let Some(current_value) = txn_buffer.get(key) {
                    if current_value.version() > *read_version {
                        println!("Conflict Detail: Committed RW on '{}' (Tx {} read ver {}, Buffer ver {})", key, committing_tx_id, read_version, current_value.version());
                        conflicts.insert(key.clone(), ConflictType::CommittedReadWrite);
                    }
                } else {
                    // Heuristic: Assume conflict if deleted after read (requires read_version > 0)
                    if *read_version > 0 {
                        println!("Conflict Detail: Committed RD on '{}' (Tx {} read ver {}, Buffer deleted)", key, committing_tx_id, read_version);
                        conflicts.insert(key.clone(), ConflictType::CommittedReadDelete);
                    }
                }
            }

            // Write Validation
            for (key, _change) in write_set {
                if let Some(current_value) = txn_buffer.get(key) {
                    // Check for WW conflict based on read version (if read) or start time (if not read)
                    let baseline_version = read_set.get(key).copied().unwrap_or(committing_tx_id);
                    if current_value.version() > baseline_version {
                        println!("Conflict Detail: Committed WW (RR) on '{}' (Tx {} baseline ver {}, Buffer ver {})", key, committing_tx_id, baseline_version, current_value.version());
                        conflicts.insert(key.clone(), ConflictType::CommittedWriteWrite);
                    }
                } else {
                    // Item is in write_set but not in buffer (potential insert or write on deleted item)
                    // CommittedWriteDelete check remains difficult without more history.
                }
            }
        }
        TransactionIsolation::Serializable => {
            // This case should not be reached as Serializable validation is handled by DependencyTracker::validate_serializability
            println!(
                "Error: detect_conflicts called unexpectedly for Serializable isolation level."
            );
            // Return empty conflicts for now, but this indicates a logic error in Transaction::commit
            // return Err(Error::Other("detect_conflicts called for Serializable".to_string()));
        }
    }

    Ok(conflicts)
}
