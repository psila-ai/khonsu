/// Defines how conflicts should be resolved during transaction commit.
///
/// When a transaction attempts to commit and conflicts are detected with
/// concurrently committed transactions, the configured `ConflictResolution`
/// strategy determines how the system should proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Append the new data from the committing transaction to the existing data
    /// in the transaction buffer for the conflicting key. This strategy is
    /// typically used for append-only data structures or logs.
    Append,
    /// Ignore the conflicting change from the committing transaction. The data
    /// in the transaction buffer remains unchanged for the conflicting key.
    Ignore,
    /// Replace the existing data in the transaction buffer with the new data
    /// from the committing transaction for the conflicting key.
    Replace,
    /// Fail the transaction if a conflict is detected. The transaction is
    /// aborted, and none of its changes are applied.
    Fail,
}

// Need to add this module to src/conflict/mod.rs later.
