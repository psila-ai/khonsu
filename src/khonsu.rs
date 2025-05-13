use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::TransactionIsolation;
use crate::conflict::resolution::ConflictResolution;
use crate::data_store::txn_buffer::TxnBuffer;
use crate::dependency_tracking::DependencyTracker;
use crate::storage::Storage;
use crate::transaction::Transaction;

#[cfg(feature = "distributed")]
use crate::prelude::dist_config::KhonsuDistConfig;
#[cfg(feature = "distributed")]
use crate::prelude::manager::DistributedCommitManager;

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

    #[cfg(feature = "distributed")]
    /// The manager for distributed commit functionality.
    distributed_manager: Option<DistributedCommitManager>,
}

impl Khonsu {
    /// Creates a new Khonsu STM instance.
    ///
    /// This function initializes the core components of the STM system, including
    /// the transaction buffer, transaction counter, storage interface, and
    /// dependency tracker.
    ///
    /// When the `distributed` feature is enabled, it also initializes and starts
    /// the `DistributedCommitManager`.
    ///
    /// # Arguments
    ///
    /// * `storage` - An `Arc` to an implementation of the `Storage` trait,
    ///   responsible for persisting committed data.
    /// * `default_isolation_level` - The default `TransactionIsolation` level
    ///   to use for new transactions created by this instance.
    /// * `default_conflict_resolution` - The default `ConflictResolution` strategy
    ///   to use for new transactions created by this instance.
    /// * `config` - Optional `KhonsuConfig` for distributed mode (required if `distributed` feature is enabled).
    ///
    /// # Returns
    ///
    /// A new `Khonsu` instance.
    ///
    /// # Examples
    ///
    /// ```no_run,ignore
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    /// use std::path::PathBuf;
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
    ///
    /// let storage = Arc::new(MockStorage::new());
    /// let isolation = TransactionIsolation::Serializable;
    /// let resolution = ConflictResolution::Fail;
    ///
    /// // For non-distributed usage:
    /// let khonsu = Khonsu::new(storage.clone(), isolation, resolution, None);
    ///
    /// // For distributed usage (with 'distributed' feature enabled):
    /// let config = KhonsuConfig {
    ///     node_id: 1,
    ///     cluster_config: ClusterConfig::new(vec![1, 2, 3]),
    ///     peer_addrs: HashMap::from([(2, "http://127.0.0.1:50052".to_string()), (3, "http://127.0.0.1:50053".to_string())]),
    ///     storage_path: PathBuf::from("/tmp/khonsu_node1_rocksdb"),
    /// };
    /// let khonsu_distributed = Khonsu::new(storage, isolation, resolution, Some(config));
    /// ```
    pub fn new(
        storage: Arc<dyn Storage>,
        default_isolation_level: TransactionIsolation,
        default_conflict_resolution: ConflictResolution,
        #[cfg(feature = "distributed")] config: Option<KhonsuDistConfig>,
    ) -> Self {
        let txn_buffer = Arc::new(TxnBuffer::new());
        let transaction_counter = Arc::new(AtomicU64::new(0));
        let dependency_tracker = Arc::new(DependencyTracker::new());

        #[cfg(feature = "distributed")]
        {
            if let Some(config) = config {
                let distributed_manager = DistributedCommitManager::new(
                    config.node_id,
                    config.cluster_config,
                    config.peer_addrs,
                    &config.storage_path,
                    txn_buffer.clone(),
                    dependency_tracker.clone(),
                    storage.clone(),
                )
                .expect("Failed to create DistributedCommitManager"); // TODO: Handle errors properly

                Self {
                    txn_buffer,
                    transaction_counter,
                    storage,
                    default_isolation_level,
                    default_conflict_resolution,
                    dependency_tracker,
                    distributed_manager: Some(distributed_manager),
                }
            } else {
                // Distributed feature enabled but no config provided
                Self {
                    txn_buffer,
                    transaction_counter,
                    storage,
                    default_isolation_level,
                    default_conflict_resolution,
                    dependency_tracker,
                    distributed_manager: None,
                }
            }
        }
        #[cfg(not(feature = "distributed"))]
        {
            // Distributed feature not enabled
            Self {
                txn_buffer,
                transaction_counter,
                storage,
                default_isolation_level,
                default_conflict_resolution,
                dependency_tracker,
            }
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
    /// ```no_run,ignore
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    /// use std::path::Path;
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

        #[cfg(feature = "distributed")]
        let decision_receiver = self
            .distributed_manager
            .as_ref()
            .map(|m| m.create_decision_receiver(transaction_id));

        // Create a new Transaction instance, passing the necessary information.
        Transaction::new(
            transaction_id,
            self.default_isolation_level,
            self.txn_buffer.clone(),
            self.transaction_counter.clone(),
            self.storage.clone(),
            self.default_conflict_resolution,
            self.dependency_tracker.clone(),
            #[cfg(feature = "distributed")]
            self.distributed_manager
                .as_ref()
                .map(|m| m.get_transaction_sender()), // Pass sender to manager
            #[cfg(feature = "distributed")]
            decision_receiver, // Pass the decision receiver
        )
    }
}

#[cfg(feature = "distributed")]
impl Khonsu {
    /// Provides access to the distributed commit manager if the feature is enabled.
    #[cfg(feature = "distributed")]
    pub fn distributed_manager(&self) -> Option<&DistributedCommitManager> {
        self.distributed_manager.as_ref()
    }
}
