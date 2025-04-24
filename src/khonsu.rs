use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::data_store::txn_buffer::TxnBuffer;
use crate::errors::Result;
use crate::transaction::Transaction;
use crate::TransactionIsolation;
use crate::conflict::resolution::ConflictResolution;
use crate::storage::Storage; // Import the Storage trait

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
            Arc::clone(&self.storage), // Pass a clone of the storage Arc
            self.default_conflict_resolution,
        )
    }

    // TODO: Add methods for registering the TwoPhaseCommitParticipant trait if needed externally.
}
