use ahash::AHashMap as HashMap;
use parking_lot::RwLock;

use crate::data_store::versioned_value::VersionedValue;

/// The key type for data items in the store. Using String for now.
///
/// This type alias defines the type used for keys to identify data items
/// within the `TxnBuffer`. Currently, it is set to `String`.
type DataKey = String;

/// The Transaction Buffer (TxnBuffer)
///
/// This struct represents the in-memory data store for the Software Transactional
/// Memory (STM) system. It holds the current state of the data that active
/// transactions operate on. The data is stored as a map of `DataKey` to
/// `VersionedValue`, protected by a `parking_lot::RwLock` for concurrent access.
pub struct TxnBuffer {
    data: RwLock<HashMap<DataKey, VersionedValue>>,
}

impl Default for TxnBuffer {
    /// Creates a new, empty `TxnBuffer` with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl TxnBuffer {
    /// Creates a new, empty `TxnBuffer`.
    ///
    /// Initializes an empty in-memory data buffer protected by a reader-writer lock.
    ///
    /// # Returns
    ///
    /// A new, empty `TxnBuffer` instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    ///
    /// let txn_buffer = TxnBuffer::new();
    /// ```
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Retrieves a `VersionedValue` from the buffer based on the key.
    ///
    /// This method provides read access to the data in the transaction buffer.
    /// It acquires a read lock on the internal data map.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the `DataKey` of the data item to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Some(VersionedValue)` if the key exists in the buffer,
    /// otherwise returns `None`. The returned `VersionedValue` is cloned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # let txn_buffer = TxnBuffer::new();
    /// let key = "my_data".to_string();
    ///
    /// // Assume some data has been inserted into the buffer.
    /// // let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    /// // let value_array = Int32Array::from(vec![10]);
    /// // let record_batch = RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap();
    /// // let versioned_value = VersionedValue::new(Arc::new(record_batch), 1);
    /// // txn_buffer.insert(key.clone(), versioned_value);
    ///
    /// match txn_buffer.get(&key) {
    ///     Some(versioned_value) => {
    ///         println!("Retrieved data for key {}: {:?}", key, versioned_value.data());
    ///     }
    ///     None => {
    ///         println!("Key {} not found in buffer.", key);
    ///     }
    /// }
    /// ```
    pub fn get(&self, key: &DataKey) -> Option<VersionedValue> {
        let data = self.data.read();
        data.get(key).cloned()
    }

    /// Inserts or updates a `VersionedValue` in the buffer.
    ///
    /// This method inserts a new data item or updates an existing one with the
    /// provided `VersionedValue`. It acquires a write lock on the internal
    /// data map to ensure exclusive access during the modification.
    ///
    /// # Arguments
    ///
    /// * `key` - The `DataKey` for the data item.
    /// * `value` - The `VersionedValue` to insert or update.
    ///
    /// # Returns
    ///
    /// Returns `Some(VersionedValue)` containing the previous value associated
    /// with the key if it existed, otherwise returns `None`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # let txn_buffer = TxnBuffer::new();
    /// let key = "new_data".to_string();
    /// let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    /// let value_array = Int32Array::from(vec![100]);
    /// let record_batch = RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap();
    /// let versioned_value = VersionedValue::new(Arc::new(record_batch), 1);
    ///
    /// match txn_buffer.insert(key.clone(), versioned_value) {
    ///     Some(previous_value) => {
    ///         println!("Updated data for key {}. Previous version: {}", key, previous_value.version());
    ///     }
    ///     None => {
    ///         println!("Inserted data for key {}.", key);
    ///     }
    /// }
    /// ```
    pub fn insert(&self, key: DataKey, value: VersionedValue) -> Option<VersionedValue> {
        let mut data = self.data.write();
        data.insert(key, value)
    }

    /// Removes a value from the buffer based on the key.
    ///
    /// This method removes the data item associated with the given `key` from
    /// the transaction buffer. It acquires a write lock on the internal data map.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the `DataKey` of the data item to remove.
    ///
    /// # Returns
    ///
    /// Returns `Some(VersionedValue)` containing the removed value if the key
    /// existed in the buffer, otherwise returns `None`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use khonsu::prelude::*;
    /// use std::sync::Arc;
    /// use arrow::record_batch::RecordBatch;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    ///
    /// # let txn_buffer = TxnBuffer::new();
    /// let key = "data_to_delete".to_string();
    ///
    /// // Assume some data exists in the buffer for this key.
    /// // let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
    /// // let value_array = Int32Array::from(vec![50]);
    /// // let record_batch = RecordBatch::try_new(schema, vec![Arc::new(value_array)]).unwrap();
    /// // let versioned_value = VersionedValue::new(Arc::new(record_batch), 2);
    /// // txn_buffer.insert(key.clone(), versioned_value);
    ///
    /// match txn_buffer.delete(&key) {
    ///     Some(removed_value) => {
    ///         println!("Removed data for key {}. Removed version: {}", key, removed_value.version());
    ///     }
    ///     None => {
    ///         println!("Key {} not found in buffer, nothing to delete.", key);
    ///     }
    /// }
    /// ```
    pub fn delete(&self, key: &DataKey) -> Option<VersionedValue> {
        let mut data = self.data.write();
        data.remove(key)
    }

    // TODO: Implement more sophisticated update logic for STM transactions.
}
