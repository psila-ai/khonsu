use ahash::AHashMap as HashMap;
use parking_lot::RwLock;

use crate::data_store::versioned_value::VersionedValue;

/// The key type for data items in the store. Using String for now.
type DataKey = String;

/// The Transaction Buffer (TxnBuffer)
/// This holds the in-memory state of the data for active transactions.
pub struct TxnBuffer {
    data: RwLock<HashMap<DataKey, VersionedValue>>,
}

impl TxnBuffer {
    /// Creates a new, empty `TxnBuffer`.
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Retrieves a `VersionedValue` from the buffer based on the key.
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: &DataKey) -> Option<VersionedValue> {
        let data = self.data.read();
        data.get(key).cloned()
    }

    /// Inserts or updates a `VersionedValue` in the buffer.
    /// This operation is protected by the RwLock.
    /// Returns the previous `VersionedValue` if one existed.
    pub fn insert(&self, key: DataKey, value: VersionedValue) -> Option<VersionedValue> {
        let mut data = self.data.write();
        data.insert(key, value)
    }

    /// Removes a value from the buffer based on the key.
    /// Returns the removed `VersionedValue` if one existed.
    pub fn delete(&self, key: &DataKey) -> Option<VersionedValue> {
        let mut data = self.data.write();
        data.remove(key)
    }

    // TODO: Implement more sophisticated update logic for STM transactions.
}
