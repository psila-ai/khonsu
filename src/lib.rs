#![doc(html_logo_url = "https://github.com/psila-ai/khonsu/raw/master/art/khonsu.jpg")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

//! **Khonsu: A Software Transactional Memory (STM) Library in Rust for Apache Arrow**
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
//! - **Distributed Extensible Design:** Designed with support for distributed commit in mind.
//!   - In distributed mode, Read Committed isolation is fully supported
//!   - Repeatable Read and Serializable isolation have limitations in distributed settings
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
//!
//! ## Getting Started
//!
//! To use **Khonsu**, add it as a dependency to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! khonsu = { git = "https://github.com/psila-ai/khonsu.git" }
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
//! # #[cfg(not(feature = "distributed"))]
//! let khonsu = Khonsu::new(storage, isolation, resolution);
//! # #[cfg(feature = "distributed")]
//! let khonsu = Khonsu::new(storage.clone(), isolation, resolution, None);
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
//! Research papers that inspired Khonsu are in our `README.md`.

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
/// 2PC Mechanism & Distributed Commit Integration
///
/// This module provides distributed transaction capabilities using a Two-Phase Commit (2PC) protocol
/// over a multi paxos consensus algorithm. It enables transactions to span multiple nodes while
/// maintaining ACID properties across the distributed system.
///
/// # Feature Flag
///
/// This module is only available when the `distributed` feature is enabled:
///
/// ```toml
/// [dependencies]
/// khonsu = { git = "https://github.com/psila-ai/khonsu.git", features = ["distributed"] }
/// ```
///
/// # Example: Setting up a distributed Khonsu cluster
///
/// ```no_run
/// # use std::sync::Arc;
/// # use std::collections::HashMap;
/// # use khonsu::prelude::*;
/// # use khonsu::storage::Storage;
/// # use khonsu::errors::Result;
/// # use khonsu::distributed::dist_config::KhonsuDistConfig;
/// # #[derive(Default)]
/// # struct MockStorage;
/// # impl Storage for MockStorage {
/// #     fn apply_mutations(&self, _mutations: Vec<StorageMutation>) -> Result<()> { Ok(()) }
/// # }
/// // Create a cluster configuration
/// let node_id = 1;
/// let cluster_config = ClusterConfig {
///     configuration_id: 1,
///     nodes: vec![1, 2, 3], // A 3-node cluster
///     flexible_quorum: None,
/// };
///
/// // Define peer addresses for gRPC communication
/// let mut peer_addrs = HashMap::new();
/// peer_addrs.insert(2, "127.0.0.1:50052".to_string());
/// peer_addrs.insert(3, "127.0.0.1:50053".to_string());
///
/// // Create a storage path for the distributed commit log
/// let storage_path = std::path::PathBuf::from("/tmp/khonsu-node1");
///
/// // Create a distributed configuration
/// let dist_config = KhonsuDistConfig {
///     node_id,
///     cluster_config,
///     peer_addrs,
///     storage_path,
/// };
///
/// // Create a Khonsu instance with distributed capabilities
/// let storage = Arc::new(MockStorage);
/// let khonsu = Khonsu::new(
///     storage,
///     TransactionIsolation::ReadCommitted, // Only ReadCommitted is fully supported in distributed mode
///     ConflictResolution::Fail,
///     Some(dist_config), // Pass the distributed configuration
/// );
///
/// // Now you can use khonsu for distributed transactions
/// ```
///
/// # Example: Performing a distributed transaction
///
/// ```no_run
/// # use std::sync::Arc;
/// # use khonsu::prelude::*;
/// # use arrow::record_batch::RecordBatch;
/// # use arrow::array::Int32Array;
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use khonsu::distributed::dist_config::KhonsuDistConfig;
/// # #[derive(Default)]
/// # struct MockStorage;
/// # impl Storage for MockStorage {
/// #     fn apply_mutations(&self, _mutations: Vec<StorageMutation>) -> Result<()> { Ok(()) }
/// # }
/// # let storage = Arc::new(MockStorage::default());
/// # let khonsu = Arc::new(Khonsu::new(
/// #     storage,
/// #     TransactionIsolation::ReadCommitted,
/// #     ConflictResolution::Fail,
/// #     None,
/// # ));
/// // Start a transaction
/// let mut txn = khonsu.start_transaction();
///
/// // Create some data
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("value", DataType::Int32, false),
/// ]));
/// let array = Int32Array::from(vec![100]);
/// let record_batch = RecordBatch::try_new(
///     schema,
///     vec![Arc::new(array)],
/// ).unwrap();
///
/// // Write data that will be replicated to all nodes
/// txn.write("distributed_key".to_string(), record_batch).unwrap();
///
/// // Commit the transaction - this will be replicated to all nodes in the cluster
/// match txn.commit() {
///     Ok(_) => println!("Transaction committed and replicated to all nodes"),
///     Err(e) => println!("Transaction failed: {:?}", e),
/// }
/// ```
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
    #[cfg(feature = "distributed")]
    pub use omnipaxos::ClusterConfig;
    #[cfg(feature = "distributed")]
    pub use omnipaxos::macros::*;
    #[cfg(feature = "distributed")]
    pub use omnipaxos::util::*;
}
