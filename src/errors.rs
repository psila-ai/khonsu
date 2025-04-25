use thiserror::Error;

#[derive(Error, Debug)]
pub enum KhonsuError {
    #[error("Transaction conflict detected")]
    TransactionConflict,

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Arrow manipulation error: {0}")]
    ArrowError(String),

    #[error("Invalid operation for isolation level")]
    InvalidIsolationOperation,

    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, KhonsuError>;
