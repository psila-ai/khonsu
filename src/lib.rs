pub mod errors;
pub mod data_store;
pub mod transaction;
pub mod khonsu;
pub mod storage;
pub mod twopc;
pub mod conflict;
pub mod arrow_utils;
pub mod dependency_tracking; // Declare the new module

// Re-export key types and structs for easier access
pub use errors::{Error, Result};
pub use data_store::txn_buffer::TxnBuffer;
pub use data_store::versioned_value::VersionedValue;
pub use khonsu::Khonsu;
pub use transaction::Transaction;
pub use storage::Storage;
pub use twopc::{TwoPhaseCommitParticipant, TransactionChanges, ParticipantError};
pub use conflict::resolution::ConflictResolution;
pub use dependency_tracking::DataItem; // Re-export DataItem


// Define the TransactionIsolation enum here as it's a core part of the public API
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
///
/// Transaction Isolation levels for transaction system
pub enum TransactionIsolation {
    ///
    /// [TransactionIsolation::ReadCommitted] isolation level means that always a committed value will be
    /// provided for read operations. Values are always read from in-memory cache every time a
    /// value is accessed. In other words, if the same key is accessed more than once within the
    /// same transaction, it may have different value every time since global cache memory
    /// may be updated concurrently by other threads.
    ReadCommitted,
    ///
    /// [TransactionIsolation::RepeatableRead] isolation level means that if a value was read once within transaction,
    /// then all consecutive reads will provide the same in-transaction value. With this isolation
    /// level accessed values are stored within in-transaction memory, so consecutive access to
    /// the same key within the same transaction will always return the value that was previously
    /// read or updated within this transaction. If concurrency is
    /// [TransactionConcurrency::Pessimistic], then a lock on the key will be acquired
    /// prior to accessing the value.
    RepeatableRead,
    ///
    /// [TransactionIsolation::Serializable] isolation level means that all transactions occur in a completely isolated fashion,
    /// as if all transactions in the system had executed serially, one after the other. Read access
    /// with this level happens the same way as with [TransactionIsolation::RepeatableRead] level.
    /// However, in  [TransactionConcurrency::Optimistic] mode, if some transactions cannot be
    /// serially isolated from each other, then one winner will be picked and the other
    /// transactions in conflict will result with abort.
    Serializable,
}