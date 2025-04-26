use crate::errors::Result;
use arrow::record_batch::RecordBatch; // Assuming errors module is at crate root

/// The key type for data items in the store. Using String for now.
///
/// This type alias defines the type used for keys to identify data items
/// within the storage layer. Currently, it is set to `String`.
type DataKey = String;

/// Represents a single mutation to be applied to the storage layer.
///
/// `StorageMutation` is used to describe a change (insert, update, or delete)
/// that needs to be persisted to durable storage as part of a committed transaction.
#[derive(Debug)]
pub enum StorageMutation {
    /// Insert or update a record with the given key and `RecordBatch`.
    /// If a record with the same key already exists, it should be updated.
    Insert(DataKey, RecordBatch),
    /// Delete the record with the given key.
    /// If no record with the key exists, this mutation should be a no-op.
    Delete(DataKey),
}

/// Trait for interacting with a storage layer to persist committed data.
///
/// Implementations of this trait are responsible for durably storing the data
/// managed by the Khonsu STM. The STM system will provide a list of `StorageMutation`s
/// to be applied atomically. The storage implementation should ensure that either
/// all mutations in a single `apply_mutations` call are persisted, or none are.
/// This trait should not contain any transaction-specific logic beyond applying
/// the provided mutations atomically.
///
/// Implementations must be `Send` and `Sync` to be used concurrently by multiple
/// transactions.
pub trait Storage: Send + Sync {
    /// Atomically applies the given mutations to the storage layer.
    ///
    /// This is the primary method for persisting changes from a committed transaction.
    /// The storage implementation must ensure that the entire list of `mutations`
    /// is applied as a single atomic unit. If any mutation fails, the entire
    /// operation should be rolled back or marked as failed, leaving the storage
    /// in a consistent state.
    ///
    /// # Arguments
    ///
    /// * `mutations` - A `Vec` of `StorageMutation`s to be applied.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all mutations were applied successfully.
    /// Returns `Err(KhonsuError::StorageError)` if an error occurred during
    /// the storage operation.
    ///
    /// # Errors
    ///
    /// Returns a `KhonsuError::StorageError` if the atomic application of mutations fails.
    ///
    /// # Examples
    ///
    /// Implementations of this trait would provide the actual storage logic.
    /// A mock implementation might look like this:
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use ahash::AHashMap as HashMap;
    /// use arrow::record_batch::RecordBatch;
    /// use std::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct MockStorage {
    ///     // In a real implementation, this would be persistent storage.
    ///     data: HashMap<String, RecordBatch>,
    /// }
    ///
    /// impl Storage for MockStorage {
    ///     fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
    ///         // In a real implementation, this would involve atomic writes
    ///         // to disk or a database.
    ///         println!("Applying {} mutations to storage (mock).", mutations.len());
    ///         for mutation in mutations {
    ///             match mutation {
    ///                 StorageMutation::Insert(key, record_batch) => {
    ///                     println!("  Mock Insert/Update: Key='{}'", key);
    ///                     // self.data.insert(key, record_batch); // Would need interior mutability
    ///                 }
    ///                 StorageMutation::Delete(key) => {
    ///                     println!("  Mock Delete: Key='{}'", key);
    ///                     // self.data.remove(&key); // Would need interior mutability
    ///                 }
    ///             }
    ///         }
    ///         Ok(()) // Assume success for the mock
    ///     }
    /// }
    /// ```
    fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()>;

    // Potentially other methods for reading initial state or recovery
    // fn read_records(&self, keys: &[DataKey]) -> Result<Vec<(DataKey, RecordBatch)>>;
}
