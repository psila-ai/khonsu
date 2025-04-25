use crate::errors::Result;
use arrow::record_batch::RecordBatch; // Assuming errors module is at crate root

/// The key type for data items in the store. Using String for now.
type DataKey = String;

/// Represents a single mutation to be applied to the storage layer.
#[derive(Debug)]
pub enum StorageMutation {
    /// Insert or update a record with the given key and RecordBatch.
    Insert(DataKey, RecordBatch),
    /// Delete the record with the given key.
    Delete(DataKey),
}

/// Trait for interacting with a storage layer to persist committed data.
/// Implementations of this trait should handle the atomic application of mutations
/// without containing any transaction-specific logic.
pub trait Storage: Send + Sync {
    /// Atomically applies the given mutations to the storage layer.
    /// All mutations in the vector should be applied as a single atomic unit.
    fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()>;

    // Potentially other methods for reading initial state or recovery
    // fn read_records(&self, keys: &[DataKey]) -> Result<Vec<(DataKey, RecordBatch)>>;
}
