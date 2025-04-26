use crate::data_store::txn_buffer::TxnBuffer;
use crate::errors::*;
use crate::TransactionIsolation;
use ahash::AHashMap as HashMap;
use arrow::record_batch::RecordBatch;
use log::debug;

/// Represents the type of conflict detected (against committed versions).
///
/// This enum is used to categorize the specific type of conflict that occurred
/// during transaction validation, typically against data that has been committed
/// by other transactions.
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
///
/// This function implements Optimistic Concurrency Control (OCC) validation
/// against committed versions for `ReadCommitted` and `RepeatableRead` isolation
/// levels. It compares the versions of data items read and written by the
/// committing transaction against the current versions in the `TxnBuffer`
/// to detect conflicts.
///
/// For `Serializable` isolation, conflict detection is handled separately
/// by the `DependencyTracker::validate_serializability` function.
///
/// # Arguments
///
/// * `committing_tx_id` - The unique identifier (logical start time) of the
///   transaction attempting to commit.
/// * `isolation_level` - The `TransactionIsolation` level of the committing transaction.
/// * `read_set` - A reference to the read set of the committing transaction,
///   mapping keys to the versions that were read.
/// * `write_set` - A reference to the write set of the committing transaction,
///   mapping keys to the staged changes (optional `RecordBatch` for insert/update,
///   `None` for delete).
/// * `txn_buffer` - A reference to the shared `TxnBuffer` containing the current
///   committed state of the data.
///
/// # Returns
///
/// Returns a `Result` containing a `HashMap` where keys are the data item keys
/// involved in a conflict, and values are the `ConflictType`. An empty map
/// indicates no conflicts were detected for the given isolation level.
///
/// # Errors
///
/// Returns a `KhonsuError` if an error occurs during the conflict detection process.
///
/// # Examples
///
/// ```no_run
/// use khonsu::prelude::*;
/// use std::sync::Arc;
/// use ahash::AHashMap as HashMap;
/// use arrow::record_batch::RecordBatch;
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{Schema, Field, DataType};
///
/// # let txn_buffer = Arc::new(TxnBuffer::new());
/// let committing_transaction_id = 10; // Example transaction ID
/// let isolation = TransactionIsolation::RepeatableRead;
///
/// // Example read set: read key "data_a" at version 5
/// let mut read_set: HashMap<String, u64> = HashMap::new();
/// read_set.insert("data_a".to_string(), 5);
///
/// // Example write set: write to key "data_b"
/// let mut write_set: HashMap<String, Option<RecordBatch>> = HashMap::new();
/// let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
/// let value_array = Int32Array::from(vec![123]);
/// let record_batch = RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap();
/// write_set.insert("data_b".to_string(), Some(record_batch));
///
/// // Assume some data exists in the txn_buffer with versions that might conflict.
/// // For example, if "data_a" in txn_buffer has version > 5, it's a ReadWrite conflict.
/// // If "data_b" in txn_buffer has version > committing_transaction_id, it's a WriteWrite conflict.
///
/// match detect_conflicts(
///     committing_transaction_id,
///     isolation,
///     &read_set,
///     &write_set,
///     &txn_buffer,
/// ) {
///     Ok(conflicts) => {
///         if conflicts.is_empty() {
///             println!("No conflicts detected for transaction {}.", committing_transaction_id);
///         } else {
///             println!("Conflicts detected for transaction {}: {:?}", committing_transaction_id, conflicts);
///         }
///     }
///     Err(e) => {
///         eprintln!("Error during conflict detection: {}", e);
///     }
/// }
/// ```
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
                        debug!("Conflict Detail: Committed WW (RC) on '{}' (Tx {} start {}, Buffer ver {})", key, committing_tx_id, committing_tx_id, current_value.version());
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
                        debug!("Conflict Detail: Committed RW on '{}' (Tx {} read ver {}, Buffer ver {})", key, committing_tx_id, read_version, current_value.version());
                        conflicts.insert(key.clone(), ConflictType::CommittedReadWrite);
                    }
                } else {
                    // Heuristic: Assume conflict if deleted after read (requires read_version > 0)
                    if *read_version > 0 {
                        debug!("Conflict Detail: Committed RD on '{}' (Tx {} read ver {}, Buffer deleted)", key, committing_tx_id, read_version);
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
                        debug!("Conflict Detail: Committed WW (RR) on '{}' (Tx {} baseline ver {}, Buffer ver {})", key, committing_tx_id, baseline_version, current_value.version());
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
            debug!("Error: detect_conflicts called unexpectedly for Serializable isolation level.");
            // Return empty conflicts for now, but this indicates a logic error in Transaction::commit
            // return Err(Error::Other("detect_conflicts called for Serializable".to_string()));
        }
    }

    Ok(conflicts)
}
