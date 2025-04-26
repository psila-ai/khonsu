use log::debug;
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
///
/// Khonsu provides a mechanism for managing concurrent data access and modifications
/// using a Software Transactional Memory (STM) approach. It allows multiple transactions
/// to operate concurrently on shared data, ensuring consistency and isolation.
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
    ///
    /// This function initializes the core components of the STM system, including
    /// the transaction buffer, transaction counter, storage interface, and
    /// dependency tracker.
    ///
    /// # Arguments
    ///
    /// * `storage` - An `Arc` to an implementation of the `Storage` trait,
    ///   responsible for persisting committed data.
    /// * `default_isolation_level` - The default `TransactionIsolation` level
    ///   to use for new transactions created by this instance.
    /// * `default_conflict_resolution` - The default `ConflictResolution` strategy
    ///   to use for new transactions created by this instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    ///
    /// # use parking_lot::Mutex;
    /// # use ahash::AHashMap as HashMap;
    /// # use arrow::record_batch::RecordBatch;
    /// # #[derive(Debug)] // Added Debug derive
    /// # pub struct MockStorage {
    /// #     data: Mutex<HashMap<String, RecordBatch>>,
    /// # }
    /// #
    /// # impl MockStorage {
    /// #     pub fn new() -> Self {
    /// #         Self {
    /// #             data: Mutex::new(HashMap::new()),
    /// #         }
    /// #     }
    /// #
    /// #     pub fn get(&self, key: &str) -> Option<RecordBatch> {
    /// #         let data = self.data.lock();
    /// #         data.get(key).cloned()
    /// #     }
    /// # }
    /// #
    /// # impl Storage for MockStorage {
    /// #     fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
    /// #         let mut data = self.data.lock();
    /// #         for mutation in mutations {
    /// #             match mutation {
    /// #                 StorageMutation::Insert(key, record_batch) => {
    /// #                     data.insert(key, record_batch);
    /// #                 }
    /// #                 StorageMutation::Delete(key) => {
    /// #                     data.remove(&key);
    /// #                 }
    /// #             }
    /// #         }
    /// #         Ok(())
    /// #     }
    /// # }
    /// // Assuming MockStorage, TransactionIsolation, and ConflictResolution are defined
    /// // and available in your scope.
    /// let storage = Arc::new(MockStorage::new());
    /// let isolation = TransactionIsolation::Serializable;
    /// let resolution = ConflictResolution::Fail;
    ///
    /// let khonsu = Khonsu::new(storage, isolation, resolution);
    /// ```
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
    ///
    /// This function creates a new `Transaction` instance with a unique transaction ID,
    /// inheriting the default isolation level and conflict resolution strategy
    /// configured for this `Khonsu` instance.
    ///
    /// The new transaction operates on a snapshot of the data from the transaction buffer
    /// based on the configured isolation level.
    ///
    /// # Returns
    ///
    /// A new `Transaction` instance ready for performing read, write, and delete operations.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    ///
    /// # use parking_lot::Mutex;
    /// # use ahash::AHashMap as HashMap;
    /// # use arrow::record_batch::RecordBatch;
    /// # #[derive(Debug)] // Added Debug derive
    /// # pub struct MockStorage {
    /// #     data: Mutex<HashMap<String, RecordBatch>>,
    /// # }
    /// #
    /// # impl MockStorage {
    /// #     pub fn new() -> Self {
    /// #         Self {
    /// #             data: Mutex::new(HashMap::new()),
    /// #         }
    /// #     }
    /// #
    /// #     pub fn get(&self, key: &str) -> Option<RecordBatch> {
    /// #         let data = self.data.lock();
    /// #         data.get(key).cloned()
    /// #     }
    /// # }
    /// #
    /// # impl Storage for MockStorage {
    /// #     fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
    /// #         let mut data = self.data.lock();
    /// #         for mutation in mutations {
    /// #             match mutation {
    /// #                 StorageMutation::Insert(key, record_batch) => {
    /// #                     data.insert(key, record_batch);
    /// #                 }
    /// #                 StorageMutation::Delete(key) => {
    /// #                     data.remove(&key);
    /// #                 }
    /// #             }
    /// #         }
    /// #         Ok(())
    /// #     }
    /// # }
    /// // Assuming a Khonsu instance 'khonsu' has been created.
    /// // let khonsu = Khonsu::new(...);
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    ///
    /// let mut transaction = khonsu.start_transaction();
    /// // Use the transaction to perform operations...
    /// ```
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

    /// Prepares a transaction for a two-phase commit protocol.
    ///
    /// This method is part of the `TwoPhaseCommitParticipant` trait and is called
    /// by a transaction coordinator during the prepare phase. It should validate
    /// the provided changes against the current state of the STM and determine
    /// if the transaction can be committed.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction.
    /// * `_changes` - The changes proposed by the transaction.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the transaction can be prepared, `Ok(false)` if it
    /// cannot (e.g., due to conflicts), and `Err(ParticipantError)` if an
    /// error occurs during the preparation process.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the preparation fails.
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
        debug!("Prepare transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Prepare not implemented yet".to_string(),
        ))
    }

    /// Commits a prepared transaction in a two-phase commit protocol.
    ///
    /// This method is called by a transaction coordinator during the commit phase
    /// after all participants have successfully prepared. It should atomically
    /// apply the changes associated with the given global transaction ID to the STM.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction to commit.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the transaction is successfully committed, and
    /// `Err(ParticipantError)` if an error occurs during the commit process.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the commit fails.
    fn commit_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError> {
        // TODO: Implement commit logic for a prepared transaction.
        // This should atomically apply the changes that were prepared
        // for this global_tx_id to the txn_buffer.
        debug!("Commit transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Commit not implemented yet".to_string(),
        ))
    }

    /// Aborts a prepared transaction in a two-phase commit protocol.
    ///
    /// This method is called by a transaction coordinator if any participant
    /// fails to prepare or if the coordinator decides to abort the transaction.
    /// It should discard any staged or prepared changes associated with the
    /// given global transaction ID.
    ///
    /// # Arguments
    ///
    /// * `global_tx_id` - The unique identifier for the global transaction to abort.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the transaction is successfully aborted, and
    /// `Err(ParticipantError)` if an error occurs during the abort process.
    ///
    /// # Errors
    ///
    /// Returns a `ParticipantError` if the abort fails.
    fn abort_transaction(
        &self,
        global_tx_id: Self::GlobalTransactionId,
    ) -> std::result::Result<(), ParticipantError> {
        // TODO: Implement abort logic for a prepared transaction.
        // This should discard any staged/prepared changes associated
        // with this global_tx_id.
        debug!("Abort transaction: {:?}", global_tx_id);
        Err(ParticipantError::Other(
            "Abort not implemented yet".to_string(),
        ))
    }
}
