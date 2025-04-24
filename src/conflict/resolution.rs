/// Defines how conflicts should be resolved during transaction commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Append the new data to the existing data.
    Append,
    /// Ignore the conflicting change from the committing transaction.
    Ignore,
    /// Replace the existing data with the new data.
    Replace,
    /// Fail the transaction if a conflict is detected.
    Fail,
}

// Need to add this module to src/conflict/mod.rs later.
