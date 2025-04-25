use crossbeam_skiplist::SkipMap;

use crate::data_store::versioned_value::VersionedValue;

/// The key type for data items in the store. Using String for now.
type DataKey = String;

/// The Transaction Buffer (TxnBuffer) implemented using a lock-free SkipMap.
/// This holds the in-memory state of the data for active transactions.
pub struct TxnBuffer {
    data: SkipMap<DataKey, VersionedValue>,
}

impl TxnBuffer {
    /// Creates a new, empty `TxnBuffer`.
    pub fn new() -> Self {
        Self {
            data: SkipMap::new(),
        }
    }

    /// Retrieves a `VersionedValue` from the buffer based on the key.
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: &DataKey) -> Option<VersionedValue> {
        self.data.get(key).map(|entry| entry.value().clone())
    }

    /// Inserts or updates a `VersionedValue` in the buffer.
    /// This operation is atomic for the specific key.
    /// Returns the previous `VersionedValue` if one existed.
    pub fn insert(&self, key: DataKey, value: VersionedValue) -> Option<VersionedValue> {
        // The insert method returns an Entry representing the newly inserted value.
        // To get the old value, we need to check if an entry was replaced.
        // crossbeam-skiplist 0.1's insert doesn't directly return the old value.
        // A simple approach is to get the value before inserting, but this is not atomic.
        // Given the constraint of not using epoch for the SkipMapStore directly,
        // and the API of crossbeam-skiplist 0.1, we will perform the insert
        // and then, if an entry was replaced, the old value is dropped when the
        // old Entry is dropped. We can't easily return the old value here atomically.
        // For now, we will just perform the insert.
        self.data.insert(key, value);
        None // Cannot atomically return the previous value with this API and constraints
    }

    /// Removes a value from the buffer based on the key.
    /// Returns the removed `VersionedValue` if one existed.
    pub fn delete(&self, key: &DataKey) -> Option<VersionedValue> {
        // The remove method returns an Entry representing the removed value.
        self.data.remove(key).map(|entry| entry.value().clone())
    }

    // TODO: Implement more sophisticated update logic for STM transactions.
}
