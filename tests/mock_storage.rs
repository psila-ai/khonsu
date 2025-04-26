//! A mock storage implementation for testing Khonsu.
//!
//! This module provides a simple in-memory implementation of the `Storage` trait
//! for use in Khonsu's integration and unit tests. It does not provide durable
//! storage but allows testing the STM logic without external dependencies.

use ahash::AHashMap as HashMap;
use arrow::record_batch::RecordBatch;
use std::sync::Mutex;

use khonsu::{
    errors::Result,
    storage::{Storage, StorageMutation},
}; // Assuming khonsu is the crate name

/// A mock implementation of the `Storage` trait for testing purposes.
/// Stores records in an in-memory HashMap.
pub struct MockStorage {
    data: Mutex<HashMap<String, RecordBatch>>,
}

impl Default for MockStorage {
    /// Creates a new, empty `MockStorage` instance with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl MockStorage {
    /// Creates a new `MockStorage` instance.
    ///
    /// Initializes an empty in-memory data store protected by a mutex.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieves a record from the mock storage.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the record to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Some(RecordBatch)` if the key exists, otherwise `None`.
    pub fn get(&self, key: &str) -> Option<RecordBatch> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

impl Storage for MockStorage {
    /// Applies the given mutations to the in-memory mock storage.
    ///
    /// This mock implementation simulates atomic application by acquiring a
    /// mutex lock for the duration of the operation. In a real storage
    /// implementation, this would involve durable, atomic writes.
    ///
    /// # Arguments
    ///
    /// * `mutations` - A vector of `StorageMutation`s to apply.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` assuming the in-memory operation succeeds.
    ///
    /// # Errors
    ///
    /// This mock implementation does not return errors, but a real storage
    /// implementation would return a `KhonsuError::StorageError` on failure.
    fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        for mutation in mutations {
            match mutation {
                StorageMutation::Insert(key, record_batch) => {
                    data.insert(key, record_batch);
                }
                StorageMutation::Delete(key) => {
                    data.remove(&key);
                }
            }
        }
        Ok(())
    }

    // TODO: Implement read_records if needed for test setup or validation.
}
