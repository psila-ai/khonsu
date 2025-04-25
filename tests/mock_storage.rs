use std::collections::HashMap;
use std::sync::Mutex;
use arrow::record_batch::RecordBatch;

use khonsu::{storage::{Storage, StorageMutation}, errors::Result}; // Assuming khonsu is the crate name

/// A mock implementation of the `Storage` trait for testing purposes.
/// Stores records in an in-memory HashMap.
pub struct MockStorage {
    data: Mutex<HashMap<String, RecordBatch>>,
}

impl MockStorage {
    /// Creates a new `MockStorage` instance.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Retrieves a record from the mock storage.
    pub fn get(&self, key: &str) -> Option<RecordBatch> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

impl Storage for MockStorage {
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
