#![doc(html_logo_url = "https://github.com/Jet-Engine/khonsu/raw/master/art/khonsu.jpg")]

//! **Khonsu: A Software Transactional Memory (STM) Library in Rust**
//!
//! **Khonsu** is a Rust library providing Software Transactional Memory (STM) capabilities.
//! It is designed to enable concurrent access to shared mutable data without relying
//! on traditional locking mechanisms, aiming for high performance and reduced contention.
//! **Khonsu** is built on top of an MVCC system.
//!
//! The library integrates with the [Apache Arrow](https://arrow.apache.org/) data format for efficient handling
//! of tabular data (RecordBatches) within transactions. It supports row-level
//! operations (create, update, delete) and provides configurable transaction
//! isolation levels and conflict resolution strategies.
//!
//! ## Features
//!
//! - **Software Transactional Memory:** Manage concurrent access to shared data using transactions.
//! - **Arrow Integration:** Work with data represented as Arrow RecordBatches.
//! - **Row-Level Operations:** Perform create, update, and delete operations on data within transactions.
//! - **Multiple RecordBatches per Transaction:** Handle complex transactions involving multiple data sets.
//! - **Conflict Detection:** Identify conflicts during the transaction commit phase.
//! - **Conflict Resolution Strategies:** Configure how conflicts are handled (append, ignore, replace, fail).
//! - **Transaction Rollback:** Ensure atomicity with complete rollback on transaction abort.
//! - **Configurable Isolation Levels:** Support Read Committed, Repeatable Read, and Serializable isolation.
//! - **Serializable Snapshot Isolation (SSI):** Implementation of SSI for the Serializable isolation level.
//! - **Extensible Design:** Designed with support for distributed commit in mind.
//!
//! ## Architecture Overview
//!
//! The core components of **Khonsu** include:
//!
//! - [`Khonsu`]: The main entry point for the STM system, responsible for starting transactions.
//! - [`Transaction`]: Represents an ongoing unit of work, managing its read and write sets.
//! - [`TxnBuffer`]: The in-memory buffer holding the current state of the data.
//! - [`VersionedValue`]: Wraps data items with version information for concurrency control.
//! - **DependencyTracker**: Tracks transaction dependencies for SSI validation.
//! - **Conflict Detection and Resolution** modules: Handle identifying and resolving transaction conflicts.
//! - [`Storage`]: A trait for interacting with durable storage.
//! - [`TwoPhaseCommitParticipant`]: A trait for integrating with distributed commit protocols.
//!
//! ## Getting Started
//!
//! To use **Khonsu**, add it as a dependency to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! khonsu = "0.1.0" # Replace with the actual version
//! ```
//!
//! ## Example Usage
//!
//! ```no_run
//! use khonsu::prelude::*;
//! use std::sync::Arc;
//! use arrow::record_batch::RecordBatch;
//! use arrow::array::{Int32Array, StringArray};
//! use arrow::datatypes::{Schema, Field, DataType};
//! use ahash::AHashMap as HashMap;
//!
//! // Define a mock storage for demonstration
//! #[derive(Default)]
//! struct MockStorage {
//!     data: HashMap<String, RecordBatch>,
//! }
//!
//! impl Storage for MockStorage {
//!     fn apply_mutations(&self, mutations: Vec<StorageMutation>) -> Result<()> {
//!         // In a real implementation, this would write to durable storage.
//!         // For this mock, we do nothing.
//!         Ok(())
//!     }
//! }
//!
//! // Create a Khonsu instance
//! let storage = Arc::new(MockStorage::default());
//! let isolation = TransactionIsolation::Serializable;
//! let resolution = ConflictResolution::Fail;
//! let khonsu = Khonsu::new(storage, isolation, resolution);
//!
//! // Start a transaction
//! let mut transaction = khonsu.start_transaction();
//!
//! // Prepare some data as an Arrow RecordBatch
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int32, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]));
//! let id_array = Int32Array::from(vec![1, 2]);
//! let name_array = StringArray::from(vec!["Alice", "Bob"]);
//! let record_batch = RecordBatch::try_new(
//!     schema,
//!     vec![Arc::new(id_array), Arc::new(name_array)],
//! ).unwrap();
//!
//! // Perform a write operation within the transaction
//! let key = "users".to_string();
//! transaction.write(key.clone(), record_batch).unwrap();
//!
//! // Attempt to commit the transaction
//! match transaction.commit() {
//!     Ok(_) => {
//!         println!("Transaction committed successfully.");
//!     }
//!     Err(KhonsuError::TransactionConflict) => {
//!         eprintln!("Transaction failed due to conflict.");
//!     }
//!     Err(e) => {
//!         eprintln!("Error during transaction: {}", e);
//!     }
//! }
//! ```
//!
//! ## Constraints and Considerations
//!
//! - Goal is lock-free internals, Atomic operations are used where applicable.
//! - Memory allocation is minimized, especially in performance-critical paths.
//!
//! ## References of Khonsu Implementation
//! Below you can find the research papers and various serializable checking mechanisms that is implemented in Khonsu.
//! ```text
//! @inproceedings{bailis2014highly,
//!   title={Highly available transactions: Virtues and limitations},
//!   author={Bailis, Peter and Ghodsi, Ali and Hellerstein, Joseph M and Stoica, Ion},
//!   booktitle={Proceedings of the VLDB Endowment},
//!   volume={7},
//!   number={3},
//!   pages={245--256},
//!   year={2014},
//!   organization={VLDB Endowment}
//! }
//!
//! @article{fekete2005serializable,
//!   title={Serializable isolation for snapshot databases},
//!   author={Fekete, Alan and Greenwood, David and Kingston, Maurice and Rice, Jeff and Storage, Andrew},
//!   journal={Proc. 29th VLDB Endowment},
//!   volume={32},
//!   pages={12},
//!   year={2005}
//! }
//!
//! @article{herlihy2003composable,
//!   title={Composable memory transactions},
//!   author={Herlihy, Maurice and Luchangco, Victor and Moir, Mark and Scherer, William N},
//!   journal={ACM SIGPLAN Notices},
//!   volume={38},
//!   number={10},
//!   pages={80--96},
//!   year={2003},
//!   publisher={ACM}
//! }
//! ```

///
/// Arrow in-mem data management utilities.
pub mod arrow_utils;
///
/// Conflict resolution system.
pub mod conflict;
///
/// Transactional variable storage layer.
pub mod data_store;
///
/// Inter/Intra data dependency tracking
pub mod dependency_tracking;
///
/// 2PC Mechanism & Distributed Commit Integration
#[cfg(feature = "distributed")]
pub mod distributed;
///
/// Khonsu Errors
pub mod errors;
///
/// Isolation Level definitions
pub mod isolation;
///
/// Khonsu - Transaction Management System
pub mod khonsu;
///
/// Storage agnostic pluggable commit mechanism
pub mod storage;
///
/// Base transaction system
pub mod transaction;

// Re-export key types and structs for easier access
pub use conflict::detection::*;
pub use conflict::resolution::ConflictResolution;
pub use data_store::txn_buffer::TxnBuffer;
pub use data_store::versioned_value::VersionedValue;
pub use dependency_tracking::DataItem;
pub use errors::{KhonsuError, Result};
pub use isolation::TransactionIsolation;
pub use khonsu::Khonsu;
pub use storage::*;
pub use transaction::Transaction;

///
/// Prelude of the Khonsu.
pub mod prelude {
    pub use crate::conflict::detection::*;
    pub use crate::conflict::resolution::*;
    pub use crate::data_store::txn_buffer::*;
    pub use crate::data_store::versioned_value::*;
    pub use crate::dependency_tracking::*;
    pub use crate::errors::*;
    pub use crate::isolation::*;
    pub use crate::khonsu::Khonsu;
    pub use crate::storage::*;
    pub use crate::transaction::Transaction;
    #[cfg(feature = "distributed")]
    pub use crate::distributed::*;
}
