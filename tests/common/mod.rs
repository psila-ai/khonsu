//! Common utilities for Khonsu integration tests.

use ahash::AHashMap as HashMap;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use khonsu::{
    conflict::resolution::ConflictResolution,
    errors::Result,
    storage::{Storage, StorageMutation},
    Khonsu, TransactionIsolation,
};
use std::sync::{Arc, Mutex};

// --- MockStorage ---

/// A mock implementation of the `Storage` trait for testing purposes.
/// Stores records in an in-memory HashMap.
#[derive(Debug)] // Added Debug derive
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
}

// --- Helper Functions ---

/// Helper function to create a Khonsu instance with MockStorage and specified isolation.
pub fn setup_khonsu(isolation: TransactionIsolation) -> Arc<Khonsu> {
    let storage = Arc::new(MockStorage::new()); // Wrap MockStorage in Arc for dyn Storage
    Arc::new(Khonsu::new(
        storage,
        isolation,
        ConflictResolution::Fail, // Default conflict resolution
    ))
}

/// Helper function to create a simple schema.
pub fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

/// Helper function to create a RecordBatch.
pub fn create_record_batch(ids: Vec<i64>, values: Vec<Option<&str>>) -> RecordBatch {
    let schema = create_test_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .unwrap()
}
