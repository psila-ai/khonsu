use thiserror::Error;

/// Represents the possible errors that can occur within the Khonsu STM system.
///
/// This enum provides a centralized way to handle and categorize errors,
/// making it easier to understand and debug issues that arise during
/// transaction processing, storage interactions, or data manipulation.
#[derive(Error, Debug)]
pub enum KhonsuError {
    /// Indicates that a transaction conflict was detected during the commit process.
    /// This typically occurs when concurrent transactions attempt to access or
    /// modify the same data in a way that violates the configured isolation level.
    #[error("Transaction conflict detected")]
    TransactionConflict,

    /// Represents an error that occurred during interaction with the underlying storage layer.
    /// This could include issues with reading, writing, or applying mutations to durable storage.
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Represents an error that occurred during interaction with the serialization.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Indicates the rollback
    #[error("Rollback: {0}")]
    RollbackError(String),

    /// Indicates an error during manipulation or processing of Arrow data (RecordBatches).
    /// This could arise from issues with schema compatibility, data conversion, or other Arrow-related operations.
    #[error("Arrow manipulation error: {0}")]
    ArrowError(String),

    /// Signifies that an attempted operation is not valid for the current transaction's isolation level.
    /// For example, attempting to perform a specific type of read or write that is restricted
    /// by the chosen isolation level.
    #[error("Invalid operation for isolation level")]
    InvalidIsolationOperation,

    /// A general-purpose error variant for capturing other unexpected issues.
    /// It includes a descriptive string to provide more context about the error.
    #[error("Other error: {0}")]
    Other(String),
}

/// A convenient type alias for the standard `Result` with `KhonsuError` as the error type.
///
/// This alias simplifies function signatures throughout the Khonsu library,
/// making it clearer that a function can return a value of type `T` or a `KhonsuError`.
pub type Result<T> = std::result::Result<T, KhonsuError>;
