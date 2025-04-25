use std::sync::Arc;
use arrow::record_batch::RecordBatch;

/// Represents a data value with an associated version/timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedValue {
    /// The actual data, likely an Arrow RecordBatch.
    data: Arc<RecordBatch>,
    /// The version or timestamp of the transaction that last committed this value.
    version: u64,
}

impl VersionedValue {
    /// Creates a new `VersionedValue`.
    pub fn new(data: Arc<RecordBatch>, version: u64) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the data.
    pub fn data(&self) -> &Arc<RecordBatch> {
        &self.data
    }

    /// Returns the version of the value.
    pub fn version(&self) -> u64 {
        self.version
    }
}
