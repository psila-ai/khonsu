use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Represents a data value with an associated version/timestamp.
///
/// In the Software Transactional Memory (STM) system, data items stored
/// in the transaction buffer are wrapped in `VersionedValue` to track
/// the version (commit timestamp) at which the data was last modified.
/// This is essential for concurrency control and conflict detection.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedValue {
    /// The actual data, likely an Arrow RecordBatch.
    data: Arc<RecordBatch>,
    /// The version or timestamp of the transaction that last committed this value.
    version: u64,
}

impl VersionedValue {
    /// Creates a new `VersionedValue`.
    ///
    /// Associates a data `RecordBatch` with a specific version or timestamp.
    ///
    /// # Arguments
    ///
    /// * `data` - An `Arc` to the `RecordBatch` containing the data.
    /// * `version` - The version or timestamp associated with this data.
    ///
    /// # Returns
    ///
    /// A new `VersionedValue` instance.
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
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    /// let id_array = Int32Array::from(vec![1]);
    /// let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();
    ///
    /// let version = 1;
    /// let versioned_value = VersionedValue::new(Arc::new(record_batch), version);
    ///
    /// println!("Created VersionedValue with version: {}", versioned_value.version());
    /// ```
    pub fn new(data: Arc<RecordBatch>, version: u64) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the data.
    ///
    /// Provides access to the underlying `Arrow RecordBatch`.
    ///
    /// # Returns
    ///
    /// A reference to the `Arc<RecordBatch>` containing the data.
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
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    /// # let id_array = Int32Array::from(vec![1]);
    /// # let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();
    /// # let version = 1;
    /// # let versioned_value = VersionedValue::new(Arc::new(record_batch), version);
    ///
    /// let data = versioned_value.data();
    /// println!("Data schema: {:?}", data.schema());
    /// ```
    pub fn data(&self) -> &Arc<RecordBatch> {
        &self.data
    }

    /// Returns the version of the value.
    ///
    /// Provides access to the version or timestamp associated with this data.
    ///
    /// # Returns
    ///
    /// The version of the value as a `u64`.
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
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    /// # let id_array = Int32Array::from(vec![1]);
    /// # let record_batch = RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();
    /// # let version = 1;
    /// # let versioned_value = VersionedValue::new(Arc::new(record_batch), version);
    ///
    /// let version = versioned_value.version();
    /// println!("Value version: {}", version);
    /// ```
    pub fn version(&self) -> u64 {
        self.version
    }
}
