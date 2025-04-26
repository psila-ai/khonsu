use crate::errors::KhonsuError;
use arrow::record_batch::RecordBatch;

/// Represents the changes made by a transaction that need to be
/// communicated during the two-phase commit process.
///
/// This type alias defines the structure used to represent the modifications
/// (inserts, updates, and deletes) performed by a transaction. It is used
/// when a Khonsu instance acts as a participant in a two-phase commit protocol.
/// Each tuple in the vector represents a change to a specific data item identified
/// by its `String` key. `Some(RecordBatch)` indicates an insertion or update
/// with the provided data, while `None` indicates a deletion.
pub type TransactionChanges = Vec<(String, Option<RecordBatch>)>; // (Key, Some(RecordBatch) for insert/update, None for delete)

/// Error type for Two-Phase Commit Participant operations.
///
/// This enum encapsulates potential errors that can occur when a Khonsu instance
/// participates in a two-phase commit protocol, including internal Khonsu errors
/// and other participant-specific issues.
#[derive(Debug, thiserror::Error)]
pub enum ParticipantError {
    /// An internal Khonsu error occurred during a participant operation.
    #[error("Participant error: {0}")]
    KhonsuError(#[from] KhonsuError),
    /// A general-purpose error variant for other participant-specific issues.
    #[error("Other participant error: {0}")]
    Other(String),
}

/// Trait for a participant in a two-phase commit protocol.
///
/// The `TwoPhaseCommitParticipant` trait defines the interface that a Khonsu
/// instance (or any other component managing a local state) must implement
/// to participate in a distributed two-phase commit protocol coordinated by
/// an external entity. This allows Khonsu to be integrated into distributed
/// database systems or other distributed transaction frameworks.
///
/// Implementations must be `Send` and `Sync` to be used concurrently.
pub trait TwoPhaseCommitParticipant: Send + Sync {
    /// The type used to identify a distributed transaction.
    ///
    /// This associated type represents the unique identifier assigned by the
    /// distributed commit coordinator to a global transaction. It must be
    /// `Send`, `Sync`, `Clone`, and implement `Debug`.
    type GlobalTransactionId: Send + Sync + Clone + std::fmt::Debug;

    /// Phase 1: Prepare the transaction.
    ///
    /// This method is called by the distributed commit coordinator during the
    /// prepare phase. The participant should validate the provided `changes`
    /// against its local state and ensure that it can commit the transaction.
    /// This typically involves checking for conflicts with other concurrent
    /// transactions and potentially reserving resources.
    ///
    /// If the participant can successfully prepare, it should return `Ok(true)`.
    /// If it detects a local conflict that prevents preparation (e.g., due to
    /// its isolation level), it should return `Ok(false)`. If a fundamental
    /// error occurs during the preparation process, it should return an `Err`.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction.
    /// * `changes` - The `TransactionChanges` representing the modifications
    ///   to be applied by this participant.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the participant is prepared to commit.
    /// Returns `Ok(false)` if the participant cannot prepare (e.g., local conflict).
    /// Returns `Err(ParticipantError)` if a fundamental error occurred.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the preparation fails due to an internal
    /// error or other participant-specific issue.
    ///
    /// # Examples
    ///
    /// Implementations would provide the actual prepare logic. A mock might look like:
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use arrow::record_batch::RecordBatch;
    ///
    /// #[derive(Debug, Clone)]
    /// struct MockGlobalTxId(u64);
    ///
    /// struct MockParticipant;
    ///
    /// impl TwoPhaseCommitParticipant for MockParticipant {
    ///     type GlobalTransactionId = MockGlobalTxId;
    ///
    ///     fn prepare_transaction(
    ///         &self,
    ///         global_tx_id: Self::GlobalTransactionId,
    ///         changes: TransactionChanges,
    ///     ) -> std::result::Result<bool, ParticipantError> {
    ///         println!("MockParticipant: Preparing transaction {:?}", global_tx_id);
    ///         // Simulate validation logic
    ///         if changes.is_empty() {
    ///             println!("MockParticipant: Prepared transaction {:?}", global_tx_id);
    ///             Ok(true) // Prepared successfully
    ///         } else {
    ///             println!("MockParticipant: Cannot prepare transaction {:?} (simulated conflict)", global_tx_id);
    ///             Ok(false) // Cannot prepare
    ///         }
    ///     }
    ///
    ///     fn commit_transaction(
    ///         &self,
    ///         global_tx_id: Self::GlobalTransactionId,
    ///     ) -> std::result::Result<(), ParticipantError> {
    ///         println!("MockParticipant: Committing transaction {:?}", global_tx_id);
    ///         // Simulate atomic application of changes
    ///         println!("MockParticipant: Committed transaction {:?}", global_tx_id);
    ///         Ok(())
    ///     }
    ///
    ///     fn abort_transaction(
    ///         &self,
    ///         global_tx_id: Self::GlobalTransactionId,
    ///     ) -> std::result::Result<(), ParticipantError> {
    ///         println!("MockParticipant: Aborting transaction {:?}", global_tx_id);
    ///         // Simulate discarding staged changes
    ///         println!("MockParticipant: Aborted transaction {:?}", global_tx_id);
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn prepare_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
        changes: TransactionChanges,
    ) -> std::result::Result<bool, ParticipantError>;

    /// Phase 2: Commit the prepared transaction.
    ///
    /// This method is called by the distributed commit coordinator during the
    /// commit phase, after all participants have successfully prepared. The
    /// participant should atomically apply the changes that were previously
    /// prepared for the given `global_tx_id`.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction to commit.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the transaction is successfully committed.
    /// Returns `Err(ParticipantError)` if an error occurs during the commit process.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the commit fails.
    fn commit_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError>;

    /// Phase 2: Abort the prepared transaction.
    ///
    /// This method is called by the distributed commit coordinator if any participant
    /// fails to prepare or if the coordinator decides to abort the transaction.
    /// The participant should discard any staged or prepared changes associated
    /// with the given `global_tx_id`.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction to abort.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the transaction is successfully aborted.
    /// Returns `Err(ParticipantError)` if an error occurs during the abort process.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the abort fails.
    fn abort_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError>;

    // Potentially methods for querying state or handling recovery
    // fn get_state(&self) -> State;
}

// Need to add this module to src/lib.rs later.
