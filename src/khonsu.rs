use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::conflict::resolution::ConflictResolution;
use crate::data_store::txn_buffer::TxnBuffer;
use crate::dependency_tracking::DependencyTracker;
use crate::storage::Storage; // Import the Storage trait
use crate::transaction::Transaction;
use crate::{
    ParticipantError, TransactionChanges, TransactionIsolation, TwoPhaseCommitParticipant,
}; // Import DependencyTracker

/// Konshu Prelude
pub mod prelude {
    pub use crate::conflict::resolution::*;
    pub use crate::data_store::txn_buffer::*;
    pub use crate::dependency_tracking::*;
    pub use crate::errors::*;
    pub use crate::storage::*;
    pub use crate::transaction::*;
    pub use crate::*;
}

/// The main entry point for the Khonsu Software Transactional Memory system.
pub struct Khonsu {
    /// The transaction buffer holding the current in-memory state of the data.
    txn_buffer: Arc<TxnBuffer>,
    /// A globally increasing counter for generating unique transaction IDs.
    transaction_counter: Arc<AtomicU64>,
    /// The storage implementation for persisting committed data.
    storage: Arc<dyn Storage>,
    /// The default isolation level for new transactions.
    default_isolation_level: TransactionIsolation,
    /// The default conflict resolution strategy for new transactions.
    default_conflict_resolution: ConflictResolution,
    /// Tracks dependencies between transactions for serializability.
    pub dependency_tracker: Arc<DependencyTracker>, // Made public for testing
}

impl Khonsu {
    /// Creates a new Khonsu STM instance.
    pub fn new(
        storage: Arc<dyn Storage>,
        default_isolation_level: TransactionIsolation,
        default_conflict_resolution: ConflictResolution,
    ) -> Self {
        Self {
            txn_buffer: Arc::new(TxnBuffer::new()),
            transaction_counter: Arc::new(AtomicU64::new(0)),
            storage,
            default_isolation_level,
            default_conflict_resolution,
            dependency_tracker: Arc::new(DependencyTracker::new()), // Initialize DependencyTracker
        }
    }

    /// Starts a new transaction.
    pub fn start_transaction(&self) -> Transaction {
        // Atomically increment the transaction counter to get a unique ID.
        let transaction_id = self.transaction_counter.fetch_add(1, Ordering::SeqCst);

        // Create a new Transaction instance, passing the necessary information.
        Transaction::new(
            transaction_id,
            self.default_isolation_level,
            Arc::clone(&self.txn_buffer), // Pass a clone of the transaction buffer Arc
            Arc::clone(&self.transaction_counter), // Pass a clone of the transaction counter Arc
            Arc::clone(&self.storage),    // Pass a clone of the storage Arc
            self.default_conflict_resolution,
            Arc::clone(&self.dependency_tracker), // Pass a clone of the dependency tracker Arc
        )
    }

    // TODO: Add methods for registering the TwoPhaseCommitParticipant trait if needed externally.
}

impl TwoPhaseCommitParticipant for Khonsu {
    // Using u64 as a simple GlobalTransactionId for now.
    type GlobalTransactionId = u64;

    fn prepare_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
        _changes: TransactionChanges,
    ) -> std::result::Result<bool, ParticipantError> {
        // TODO: Implement prepare logic.
        // This should involve validating the changes against the current state
        // in the txn_buffer, similar to the first phase of local commit.
        // Need to check for conflicts based on the isolation level.
        // If prepared, the changes might need to be temporarily stored,
        // associated with the global_tx_id, until commit or abort is called.
        println!("Prepare transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Prepare not implemented yet".to_string(),
        ))
    }

    fn commit_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError> {
        // TODO: Implement commit logic for a prepared transaction.
        // This should atomically apply the changes that were prepared
        // for this global_tx_id to the txn_buffer.
        println!("Commit transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Commit not implemented yet".to_string(),
        ))
    }

    fn abort_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError> {
        // TODO: Implement abort logic for a prepared transaction.
        // This should discard any staged/prepared changes associated
        // with this global_tx_id.
        println!("Abort transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Abort not implemented yet".to_string(),
        ))
    }
}
