use crate::errors::Result; // Assuming errors module is at crate root
use crate::errors::Error; // Assuming errors module is at crate root
use arrow::record_batch::RecordBatch;

/// Represents the changes made by a transaction that need to be
/// communicated during the two-phase commit process.
// This could be a more sophisticated structure detailing inserts, updates, and deletes.
pub type TransactionChanges = Vec<(String, Option<RecordBatch>)>; // (Key, Some(RecordBatch) for insert/update, None for delete)

/// Error type for Two-Phase Commit Participant operations.
#[derive(Debug, thiserror::Error)]
pub enum ParticipantError {
    #[error("Participant error: {0}")]
KhonsuError(#[from] Error),
    #[error("Other participant error: {0}")]
    Other(String),
}

/// Trait for a participant in a two-phase commit protocol.
/// The Khonsu instance on each node will implement this trait
/// to integrate with a distributed commit coordinator.
pub trait TwoPhaseCommitParticipant: Send + Sync {
    /// The type used to identify a distributed transaction.
    type GlobalTransactionId: Send + Sync + Clone + std::fmt::Debug;

    /// Phase 1: Prepare the transaction.
    /// The participant validates the changes locally and ensures they can be applied.
    /// Returns `Ok(true)` if prepared, `Ok(false)` if cannot prepare (e.g., local conflict),
    /// or an error if a fundamental issue occurred.
    fn prepare_transaction(&self, global_tx_id: Self::GlobalTransactionId, changes: TransactionChanges) -> std::result::Result<bool, ParticipantError>;

    /// Phase 2: Commit the prepared transaction.
    /// The participant atomically applies the changes that were previously prepared.
    fn commit_transaction(&self, global_tx_id: Self::GlobalTransactionId) -> std::result::Result<(), ParticipantError>;

    /// Phase 2: Abort the prepared transaction.
    /// The participant discards the staged changes for the given transaction.
    fn abort_transaction(&self, global_tx_id: Self::GlobalTransactionId) -> std::result::Result<(), ParticipantError>;

    // Potentially methods for querying state or handling recovery
    // fn get_state(&self) -> State;
}

// Need to add this module to src/lib.rs later.
