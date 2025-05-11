use ahash::{AHashMap as HashMap, AHashSet as HashSet}; // Keep HashSet import
use arrow::record_batch::RecordBatch;
use log::debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "distributed")]
use crossbeam_channel as channel; // Use crossbeam for channels
#[cfg(feature = "distributed")]
use crate::distributed::channel_ext::{SenderExt};

use crate::conflict::detection::detect_conflicts;
use crate::conflict::resolution::ConflictResolution;
use crate::data_store::txn_buffer::TxnBuffer;
use crate::data_store::versioned_value::VersionedValue;
use crate::dependency_tracking::{DataItem, DependencyTracker}; // Keep DependencyTracker
use crate::errors::{KhonsuError, Result};
use crate::storage::*;
use crate::TransactionIsolation;

#[cfg(feature = "distributed")]
/// Represents the outcome of a distributed transaction commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedCommitOutcome {
    Committed,
    Aborted,
}

/// Represents a single transaction.
///
/// A transaction provides a mechanism for performing a series of read, write,
/// and delete operations on the data buffer atomically and in isolation.
/// Transactions are created by the `Khonsu` instance and manage their own
/// read and write sets.
pub struct Transaction {
    /// Unique identifier for the transaction (timestamp).
    id: u64,
    /// The isolation level for this transaction.
    isolation_level: TransactionIsolation,
    /// Reference to the transaction buffer.
    txn_buffer: Arc<TxnBuffer>,
    /// Reference to the global transaction counter.
    transaction_counter: Arc<AtomicU64>,
    /// Reference to the storage implementation.
    storage: Arc<dyn Storage>,
    /// The set of data items read by this transaction and their versions.
    read_set: HashMap<String, u64>,
    /// Staged changes (insertions, updates, deletions) for this transaction.
    // Using Option<RecordBatch> to represent insertions/updates (Some) and deletions (None).
    write_set: HashMap<String, Option<RecordBatch>>,
    /// The conflict resolution strategy for this transaction.
    conflict_resolution: ConflictResolution,
    /// Reference to the dependency tracker for serializability.
    dependency_tracker: Arc<DependencyTracker>,

    #[cfg(feature = "distributed")]
    /// Sender channel to the DistributedCommitManager for proposing commits.
    distributed_manager_sender: Option<crate::distributed::channel_ext::NodeSender>,
    #[cfg(feature = "distributed")]
    /// Receiver channel for the DistributedCommitManager's decision on this transaction's commit.
    decision_receiver: Option<channel::Receiver<DistributedCommitOutcome>>,
}

impl Transaction {
    /// Returns the unique identifier of the transaction.
    ///
    /// This ID is assigned when the transaction is created and is typically
    /// a monotonically increasing timestamp.
    ///
    /// # Returns
    ///
    /// The unique transaction ID as a `u64`.
    ///
    /// # Examples
    ///
    /// ```no_run
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
    /// #
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let transaction = khonsu.start_transaction();
    /// let transaction_id = transaction.id();
    /// println!("Transaction ID: {}", transaction_id);
    /// ```
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Creates a new transaction.
    ///
    /// This is typically called internally by the `Khonsu` instance when
    /// `start_transaction` is invoked. It initializes the transaction's
    /// state, including its ID, isolation level, and references to shared
    /// components like the transaction buffer and dependency tracker.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier for the transaction.
    /// * `isolation_level` - The desired `TransactionIsolation` level for this transaction.
    /// * `txn_buffer` - An `Arc` to the shared `TxnBuffer`.
    /// * `transaction_counter` - An `Arc` to the global `AtomicU64` transaction counter.
    /// * `storage` - An `Arc` to the `Storage` implementation.
    /// * `conflict_resolution` - The `ConflictResolution` strategy for this transaction.
    /// * `dependency_tracker` - An `Arc` to the shared `DependencyTracker`.
    /// * `distributed_manager_sender` - Optional sender to the DistributedCommitManager (if distributed).
    /// * `decision_receiver` - Optional receiver for the DistributedCommitManager's decision (if distributed).
    ///
    /// # Returns
    ///
    /// A new `Transaction` instance.
    pub fn new(
        id: u64,
        isolation_level: TransactionIsolation,
        txn_buffer: Arc<TxnBuffer>,
        transaction_counter: Arc<AtomicU64>,
        storage: Arc<dyn Storage>,
        conflict_resolution: ConflictResolution,
        dependency_tracker: Arc<DependencyTracker>,
        #[cfg(feature = "distributed")] distributed_manager_sender: Option<
            crate::distributed::channel_ext::NodeSender,
        >,
        #[cfg(feature = "distributed")] decision_receiver: Option<
            channel::Receiver<DistributedCommitOutcome>,
        >,
    ) -> Self {
        // Register the transaction with the tracker when it starts
        dependency_tracker.register_txn(id);
        Self {
            id,
            isolation_level,
            txn_buffer,
            transaction_counter,
            storage,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            conflict_resolution,
            dependency_tracker,
            #[cfg(feature = "distributed")]
            distributed_manager_sender,
            #[cfg(feature = "distributed")]
            decision_receiver,
        }
    }

    /// Reads data associated with a key.
    ///
    /// This method attempts to read the latest committed version of the data
    /// associated with the given `key`. If the key has been modified within
    /// this transaction's write set, the staged change is returned. Otherwise,
    /// the data is read from the shared transaction buffer.
    ///
    /// For `RepeatableRead` and `Serializable` isolation levels, the version
    /// of the read data is recorded in the transaction's read set for conflict
    /// detection during commit. For `Serializable`, the read is also recorded
    /// in the dependency tracker.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key of the data item to read.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing an `Option<Arc<RecordBatch>>`.
    /// - `Ok(Some(record_batch))` if the key exists and the data was read successfully.
    /// - `Ok(None)` if the key does not exist in the transaction buffer or write set.
    /// - `Err(KhonsuError)` if an error occurs during the read operation.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError` if the read operation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::{Int32Array, StringArray};
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # use parking_lot::Mutex;
    /// # use ahash::AHashMap as HashMap;
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
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let mut transaction = khonsu.start_transaction();
    ///
    /// let key = "my_data".to_string();
    /// match transaction.read(&key) {
    ///     Ok(Some(record_batch)) => {
    ///         println!("Read data for key {}: {:?}", key, record_batch);
    ///     }
    ///     Ok(None) => {
    ///         println!("Key {} not found.", key);
    ///     }
    ///     Err(e) => {
    ///         eprintln!("Error reading data: {}", e);
    ///     }
    /// }
    /// ```
    pub fn read(&mut self, key: &String) -> Result<Option<Arc<RecordBatch>>> {
        // Check the write set first
        if let Some(change) = self.write_set.get(key) {
            return Ok(change.as_ref().map(|rb| Arc::new(rb.clone())));
        }

        // Read from the transaction buffer
        let versioned_value = self.txn_buffer.get(key);

        match self.isolation_level {
            TransactionIsolation::ReadCommitted => {
                if let Some(value) = versioned_value {
                    // Don't record reads for ReadCommitted in read_set for OCC
                    Ok(Some(value.data().clone()))
                } else {
                    Ok(None)
                }
            }
            TransactionIsolation::RepeatableRead | TransactionIsolation::Serializable => {
                if let Some(value) = versioned_value {
                    let version = value.version();
                    self.read_set.insert(key.to_string(), version);
                    // For Serializable, record read in dependency tracker
                    if self.isolation_level == TransactionIsolation::Serializable {
                        let data_item = DataItem { key: key.clone() };
                        self.dependency_tracker
                            .record_read(self.id, data_item, version);
                    }
                    Ok(Some(value.data().clone()))
                } else {
                    // Record the fact that the key didn't exist at this point (version 0)
                    self.read_set.insert(key.to_string(), 0);
                    Ok(None)
                }
            }
        }
    }

    /// Stages a write operation for a key with the provided RecordBatch.
    ///
    /// This method stages a change to be applied to the data buffer upon
    /// successful transaction commit. The provided `record_batch` will
    /// replace any existing data associated with the `key`.
    ///
    /// For `Serializable` isolation, a write intention is immediately recorded
    /// in the dependency tracker.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the data item to write.
    /// * `record_batch` - The `RecordBatch` containing the data to write.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the write operation is staged successfully, and
    /// `Err(KhonsuError)` if an error occurs.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError` if the write operation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::{Int32Array, StringArray};
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # use parking_lot::Mutex;
    /// # use ahash::AHashMap as HashMap;
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
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let mut transaction = khonsu.start_transaction();
    ///
    /// let key = "my_data".to_string();
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("name", DataType::Utf8, false),
    /// ]));
    /// let id_array = Int32Array::from(vec![1, 2]);
    /// let name_array = StringArray::from(vec!["Alice", "Bob"]);
    /// let record_batch = RecordBatch::try_new(
    ///     schema,
    ///     vec![Arc::new(id_array), Arc::new(name_array)],
    /// ).unwrap();
    ///
    /// match transaction.write(key.clone(), record_batch) {
    ///     Ok(_) => {
    ///         println!("Staged write for key {}.", key);
    ///     }
    ///     Err(e) => {
    ///         eprintln!("Error staging write: {}", e);
    ///     }
    /// }
    /// ```
    pub fn write(&mut self, key: String, record_batch: RecordBatch) -> Result<()> {
        // Record write intention immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem { key: key.clone() }; // Clone key for data_item
            self.dependency_tracker
                .record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        // Stage the write/update in the write set.
        self.write_set.insert(key, Some(record_batch));
        Ok(())
    }

    /// Stages a delete operation for a key.
    ///
    /// This method stages the deletion of the data associated with the given
    /// `key`. The actual deletion from the data buffer occurs upon successful
    /// transaction commit.
    ///
    /// For `Serializable` isolation, a write intention (for deletion) is
    /// immediately recorded in the dependency tracker.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the key of the data item to delete.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the delete operation is staged successfully, and
    /// `Err(KhonsuError)` if an error occurs.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError` if the delete operation fails.
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
    /// #
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let mut transaction = khonsu.start_transaction();
    /// # let txn_id = transaction.id();
    ///
    /// // Perform some operations...
    /// // transaction.write(...).unwrap();
    ///
    /// // Decide to roll back
    /// transaction.delete("some_key").unwrap();
    /// transaction.rollback();
    /// println!("Transaction {} rolled back.", txn_id);
    /// ```
    pub fn delete(&mut self, key: &str) -> Result<()> {
        let key_string = key.to_string();
        // Record write intention immediately for Serializable isolation
        if self.isolation_level == TransactionIsolation::Serializable {
            let data_item = DataItem {
                key: key_string.clone(),
            }; // Clone key for data_item
            self.dependency_tracker
                .record_write(self.id, data_item, self.id); // Use txn ID as placeholder version
        }
        // Stage the deletion in the write set.
        self.write_set.insert(key_string, None);
        Ok(())
    }

    /// Attempts to commit the transaction.
    ///
    /// This is the core of the transaction lifecycle. It performs the following steps:
    /// 1. Assigns a commit timestamp to the transaction.
    /// 2. Performs validation and conflict detection based on the transaction's
    ///    isolation level (`ReadCommitted`, `RepeatableRead`, or `Serializable`).
    ///    - For `Serializable`, it performs Serializable Snapshot Isolation (SSI)
    ///      validation using the `DependencyTracker`.
    ///    - For other levels, it performs standard Optimistic Concurrency Control (OCC)
    ///      checks against the transaction buffer.
    /// 3. If conflicts are detected (and the isolation level is not `Serializable`
    ///    where SSI handles conflicts), applies the configured `ConflictResolution`
    ///    strategy (`Fail`, `Ignore`, `Replace`, `Append`).
    /// 4. If validation passes and conflicts are resolved (or none existed),
    ///    atomically applies the staged changes from the write set to the
    ///    shared transaction buffer.
    /// 5. Persists the changes to durable storage using the provided `Storage`
    ///    implementation.
    /// 6. Marks the transaction as committed in the `DependencyTracker` (for `Serializable`).
    ///
    /// If validation fails or conflicts cannot be resolved according to the
    /// strategy, the transaction is aborted.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the transaction is successfully committed.
    /// Returns `Err(KhonsuError::TransactionConflict)` if a conflict is detected
    /// and the resolution strategy is `Fail`.
    /// Returns `Err(KhonsuError)` for other errors during the commit process.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError` if the commit operation fails due to conflicts
    /// (with `Fail` resolution) or other internal errors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use khonsu::prelude::*;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::{Int32Array, StringArray};
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # use parking_lot::Mutex;
    /// # use ahash::AHashMap as HashMap;
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
    /// #
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let mut transaction = khonsu.start_transaction();
    /// # let txn_id = transaction.id();
    ///
    /// // Perform read/write/delete operations...
    /// let key = "new_data".to_string();
    /// let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    /// let value_array = Int32Array::from(vec![100]);
    /// let record_batch = RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap();
    /// transaction.write(key.clone(), record_batch).unwrap();
    ///
    /// match transaction.commit() {
    ///     Ok(_) => {
    ///         println!("Transaction {} committed successfully.", txn_id);
    ///     }
    ///     Err(KhonsuError::TransactionConflict) => {
    ///         eprintln!("Transaction {} failed due to conflict.", txn_id);
    ///     }
    ///     Err(e) => {
    ///         eprintln!("Error during transaction commit: {}", e);
    ///     }
    /// }
    /// ```
    pub fn commit(self) -> Result<()> {
        // Phase 1: Validation and Conflict Detection
        let commit_timestamp = self.transaction_counter.fetch_add(1, Ordering::SeqCst);

        // Take ownership of write_set early
        let mut write_set_to_apply = self.write_set;
        // Need the keys for validation/marking committed
        let write_set_keys: HashSet<String> = write_set_to_apply.keys().cloned().collect();

        // Perform validation based on isolation level
        let conflicts = match self.isolation_level {
            TransactionIsolation::Serializable => {
                // For Serializable, perform SSI validation ONLY.
                // We need a "pre-commit" timestamp for SSI validation before proposing to OmniPaxos.
                // Using the current transaction counter value + 1 as a potential commit timestamp.
                let potential_commit_timestamp =
                    self.transaction_counter.load(Ordering::SeqCst) + 1;
                if !self.dependency_tracker.validate_serializability(
                    self.id,
                    potential_commit_timestamp, // Use potential commit_ts for validation
                    &self.read_set,
                    &write_set_keys,
                    &self.txn_buffer,
                )? {
                    // SSI validation failed (backward or forward check).
                    self.dependency_tracker.mark_aborted(self.id); // Mark as aborted in tracker
                    return Err(KhonsuError::TransactionConflict);
                }
                // If SSI validation passes, assume no conflicts for this level locally.
                HashMap::new() // Return empty conflicts
            }
            TransactionIsolation::ReadCommitted | TransactionIsolation::RepeatableRead => {
                // For other levels, perform standard OCC conflict detection against the current buffer state.
                detect_conflicts(
                    self.id, // Pass start_ts (which is id)
                    self.isolation_level,
                    &self.read_set,
                    &write_set_to_apply, // Pass original write_set map
                    &self.txn_buffer,
                )?
            }
        };

        // If OCC checks (for non-Serializable) found conflicts:
        if !conflicts.is_empty() {
            // Standard conflict resolution logic
            match self.conflict_resolution {
                ConflictResolution::Fail => {
                    // Mark as aborted (tracker handles non-serializable cases gracefully)
                    self.dependency_tracker.mark_aborted(self.id);
                    return Err(KhonsuError::TransactionConflict);
                }
                ConflictResolution::Ignore => {
                    write_set_to_apply.retain(|key, _| !conflicts.contains_key(key));
                    debug!("Conflict detected for transaction {}. Resolution: Ignore. Filtered {} conflicting changes.", self.id, conflicts.len());
                }
                ConflictResolution::Replace => {
                    debug!("Conflict detected for transaction {}. Resolution: Replace. Conflicting changes will overwrite existing data.", self.id);
                }
                ConflictResolution::Append => {
                    debug!("Conflict detected for transaction {}. Resolution: Append. Merging conflicting changes.", self.id);
                    let mut merged_changes: HashMap<String, Option<RecordBatch>> = HashMap::new();
                    for (key, _conflict_type) in &conflicts {
                        if let Some(change) = write_set_to_apply.get(key) {
                            if let Some(new_data) = change {
                                let current_data = self.txn_buffer.get(key);
                                if let Some(existing_value) = current_data {
                                    let merged_record_batch =
                                        crate::arrow_utils::merge_record_batches(
                                            existing_value.data(),
                                            &HashMap::from([(key.clone(), Some(new_data.clone()))]),
                                            "id",
                                        )?;
                                    merged_changes.insert(key.clone(), Some(merged_record_batch));
                                } else {
                                    merged_changes.insert(key.clone(), Some(new_data.clone()));
                                }
                            } else {
                                debug!("Conflict on deletion for key {}. Append resolution not applicable.", key);
                                merged_changes.insert(key.clone(), None);
                            }
                        } else {
                            debug!("Conflict on read-only key {}. No Append resolution action needed on write set.", key);
                        }
                    }
                    for (key, merged_change) in merged_changes {
                        write_set_to_apply.insert(key, merged_change);
                    }
                }
            }
        }

        // If distributed feature is enabled, propose to OmniPaxos
        #[cfg(feature = "distributed")]
        {
            if let Some(ref distributed_manager_sender) = self.distributed_manager_sender {
                if let Some(decision_receiver) = self.decision_receiver {
                    // Get the node ID before using the sender
                    let node_id = distributed_manager_sender.node_id();
                    
                    // Create a global transaction ID
                    let global_txn_id = crate::distributed::GlobalTransactionId::new(
                        node_id,
                        self.id
                    );
                    
                    // Convert the write set to SerializableVersionedValue
                    let mut serializable_write_set = HashMap::new();
                    for (key, value_opt) in &write_set_to_apply {
                        match value_opt {
                            Some(record_batch) => {
                                // Create a VersionedValue with a temporary version
                                let versioned_value = VersionedValue::new(
                                    Arc::new(record_batch.clone()),
                                    self.id, // Use transaction ID as temporary version
                                );
                                
                                // Convert to SerializableVersionedValue
                                match crate::distributed::SerializableVersionedValue::from_versioned_value(&versioned_value) {
                                    Ok(serializable_value) => {
                                        serializable_write_set.insert(key.clone(), serializable_value);
                                    },
                                    Err(e) => {
                                        return Err(KhonsuError::SerializationError(format!(
                                            "Failed to serialize VersionedValue: {}", e
                                        )));
                                    }
                                }
                            },
                            None => {
                                // This is a deletion
                                serializable_write_set.insert(
                                    key.clone(),
                                    crate::distributed::SerializableVersionedValue::new_tombstone(self.id),
                                );
                            }
                        }
                    }
                    
                    // Get the prepare timestamp
                    let prepare_timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    
                    // Create participant nodes list
                    let participant_nodes = vec![node_id]; // For now, just include the local node
                    
                    // Convert AHashMap to std::collections::HashMap
                    let std_write_set: std::collections::HashMap<String, crate::distributed::SerializableVersionedValue> = 
                        serializable_write_set.into_iter().collect();
                    let std_read_set: std::collections::HashMap<String, u64> = 
                        self.read_set.clone().into_iter().collect();
                    
                    // Create the replicated commit message
                    let replicated_commit = crate::distributed::ReplicatedCommit::new_prepared(
                        global_txn_id,
                        std_write_set,
                        std_read_set,
                        prepare_timestamp,
                        node_id,
                        participant_nodes,
                    );

                    // Send the commit proposal to the DistributedCommitManager
                    if let Err(e) = distributed_manager_sender.send(replicated_commit) {
                        eprintln!(
                            "Error sending commit proposal to DistributedCommitManager: {:?}",
                            e
                        );
                        self.dependency_tracker.mark_aborted(self.id);
                        return Err(KhonsuError::DistributedCommitError(format!(
                            "Failed to send commit proposal: {:?}",
                            e
                        )));
                    }

                    // Wait for the decision from the DistributedCommitManager
                    match decision_receiver.recv() {
                        Ok(DistributedCommitOutcome::Committed) => {
                            debug!("Transaction {} committed via OmniPaxos.", self.id);
                            // The actual application of changes and marking as committed in tracker
                            // will happen in the DistributedCommitManager when the entry is decided.
                            Ok(())
                        }
                        Ok(DistributedCommitOutcome::Aborted) => {
                            debug!(
                                "Transaction {} aborted by DistributedCommitManager.",
                                self.id
                            );
                            // The marking as aborted in tracker will happen in the DistributedCommitManager.
                            Err(KhonsuError::TransactionConflict) // Or a specific distributed abort error
                        }
                        Err(e) => {
                            eprintln!("Error receiving DistributedCommitManager decision for transaction {}: {:?}", self.id, e);
                            self.dependency_tracker.mark_aborted(self.id);
                            Err(KhonsuError::DistributedCommitError(format!(
                                "Failed to receive DistributedCommitManager decision: {:?}",
                                e
                            )))
                        }
                    }
                } else {
                    // Distributed feature enabled but no decision receiver provided
                    eprintln!("Distributed feature enabled but no decision receiver provided for transaction {}.", self.id);
                    self.dependency_tracker.mark_aborted(self.id);
                    Err(KhonsuError::DistributedCommitError(
                        "No DistributedCommitManager decision receiver available".to_string(),
                    ))
                }
            } else {
                // Distributed feature enabled but no DistributedCommitManager sender provided
                eprintln!("Distributed feature enabled but no DistributedCommitManager sender provided for transaction {}.", self.id);
                self.dependency_tracker.mark_aborted(self.id);
                Err(KhonsuError::DistributedCommitError(
                    "No DistributedCommitManager sender available".to_string(),
                ))
            }
        }

        #[cfg(not(feature = "distributed"))]
        {
            // If distributed feature is NOT enabled, proceed with local commit
            debug!("Transaction {} committing locally.", self.id);

            // Phase 2: Apply Changes (Local)
            let mut mutations_to_persist: Vec<StorageMutation> = Vec::new();
            for (key, change) in write_set_to_apply.drain() {
                match change {
                    Some(record_batch) => {
                        // Use commit_timestamp as the version for the new value
                        let versioned_value =
                            VersionedValue::new(Arc::new(record_batch.clone()), commit_timestamp);
                        self.txn_buffer.insert(key.clone(), versioned_value);
                        mutations_to_persist.push(StorageMutation::Insert(key, record_batch));
                    }
                    None => {
                        self.txn_buffer.delete(&key.clone());
                        mutations_to_persist.push(StorageMutation::Delete(key));
                    }
                }
            }

            self.storage.apply_mutations(mutations_to_persist)?;

            // Mark as committed in tracker
            // Pass the cloned write_set_keys
            self.dependency_tracker
                .mark_committed(self.id, commit_timestamp, write_set_keys);

            Ok(())
        }
    }

    /// Aborts the transaction, discarding staged changes.
    ///
    /// This method rolls back the transaction by discarding all staged changes
    /// in the write set. The transaction buffer remains unaffected by the
    /// operations performed within this transaction.
    ///
    /// The transaction is marked as aborted in the `DependencyTracker` (for `Serializable`).
    ///
    /// # Examples
    ///
    /// ```no_run
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
    /// #
    /// # let storage = Arc::new(MockStorage::new());
    /// # let isolation = TransactionIsolation::Serializable;
    /// # let resolution = ConflictResolution::Fail;
    /// # let khonsu = Khonsu::new(storage, isolation, resolution);
    /// let mut transaction = khonsu.start_transaction();
    /// # let txn_id = transaction.id();
    ///
    /// // Perform some operations...
    /// // transaction.write(...).unwrap();
    ///
    /// // Decide to roll back
    /// transaction.delete("some_key").unwrap();
    /// transaction.rollback();
    /// println!("Transaction {} rolled back.", txn_id);
    /// ```
    pub fn rollback(self) {
        debug!("Transaction {} rolled back", self.id);
        // Mark as aborted in tracker
        self.dependency_tracker.mark_aborted(self.id);
        // The `write_set` and `read_set` are dropped when `self` is dropped.
    }
}
