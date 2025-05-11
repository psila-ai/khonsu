// This module contains the distributed commit implementation using OmniPaxos.

pub mod channel_ext; // Module for channel extensions
pub mod dist_config;
pub mod grpc_server; // Module for the gRPC server implementation
pub mod manager; // Module for the DistributedCommitManager
pub mod network; // Module for OmniPaxos network implementation
pub mod storage; // Module for OmniPaxos storage backend
pub mod twopc; // Module for the Two-Phase Commit implementation

use crate::data_store::versioned_value::VersionedValue;
use arrow::record_batch::RecordBatch;
use omnipaxos::{macros::Entry, storage::Snapshot, util::NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A serializable wrapper for VersionedValue.
///
/// Since VersionedValue doesn't implement Serialize/Deserialize,
/// we need this wrapper to make it serializable for OmniPaxos.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableVersionedValue {
    /// The version or timestamp.
    version: u64,
    /// Whether this is a tombstone (deletion marker).
    is_tombstone: bool,
    /// Schema and data for the RecordBatch, serialized as IPC format.
    /// This is empty for tombstones.
    ipc_data: Vec<u8>,
}

impl SerializableVersionedValue {
    /// Creates a new SerializableVersionedValue from a VersionedValue.
    pub fn from_versioned_value(value: &VersionedValue) -> Result<Self, crate::errors::KhonsuError> {
        // Use Arrow's IPC format to serialize the RecordBatch
        let record_batch = value.data();
        
        // Create an in-memory IPC writer
        let mut buffer = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(
                &mut buffer,
                &record_batch.schema(),
            ).map_err(|e| crate::errors::KhonsuError::ArrowError(e.to_string()))?;
            
            // Write the RecordBatch
            writer.write(record_batch)
                .map_err(|e| crate::errors::KhonsuError::ArrowError(e.to_string()))?;
            
            // Finish writing
            writer.finish()
                .map_err(|e| crate::errors::KhonsuError::ArrowError(e.to_string()))?;
        }
        
        Ok(Self {
            version: value.version(),
            is_tombstone: false,
            ipc_data: buffer,
        })
    }
    
    /// Creates a new tombstone SerializableVersionedValue.
    pub fn new_tombstone(version: u64) -> Self {
        Self {
            version,
            is_tombstone: true,
            ipc_data: Vec::new(),
        }
    }
    
    /// Converts this SerializableVersionedValue back to a VersionedValue.
    pub fn to_versioned_value(&self) -> Result<VersionedValue, crate::errors::KhonsuError> {
        if self.is_tombstone {
            // Create a tombstone VersionedValue
            let empty_schema = Arc::new(arrow::datatypes::Schema::empty());
            let empty_batch = RecordBatch::new_empty(empty_schema);
            Ok(VersionedValue::new(Arc::new(empty_batch), self.version))
        } else {
            // Use Arrow's IPC format to deserialize the RecordBatch
            let reader = arrow::ipc::reader::FileReader::try_new(
                std::io::Cursor::new(&self.ipc_data),
                None,
            ).map_err(|e| crate::errors::KhonsuError::ArrowError(e.to_string()))?;
            
            // Get the first RecordBatch
            let record_batch = reader.into_iter()
                .next()
                .ok_or_else(|| crate::errors::KhonsuError::ArrowError("No RecordBatch in IPC data".to_string()))?
                .map_err(|e| crate::errors::KhonsuError::ArrowError(e.to_string()))?;
            
            Ok(VersionedValue::new(Arc::new(record_batch), self.version))
        }
    }
    
    /// Returns the version of this value.
    pub fn version(&self) -> u64 {
        self.version
    }
    
    /// Returns whether this is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.is_tombstone
    }
}

/// Represents a globally unique transaction identifier.
///
/// This struct ensures that transaction IDs are unique across the entire cluster
/// by combining the node ID, a local transaction ID, and a timestamp.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GlobalTransactionId {
    /// The ID of the node that created this transaction.
    pub node_id: NodeId,
    /// The local transaction ID assigned by the node.
    pub local_id: u64,
    /// A timestamp for ordering and debugging purposes.
    pub timestamp: u64,
}

impl fmt::Display for GlobalTransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Txn[Node:{},ID:{},TS:{}]", self.node_id, self.local_id, self.timestamp)
    }
}

impl GlobalTransactionId {
    /// Creates a new `GlobalTransactionId`.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The ID of the node creating the transaction.
    /// * `local_id` - The local transaction ID assigned by the node.
    pub fn new(node_id: NodeId, local_id: u64) -> Self {
        Self {
            node_id,
            local_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
    
    /// Computes a hash value for this transaction ID.
    ///
    /// This can be used for consistent hashing to distribute transactions
    /// across the cluster.
    pub fn hash_value(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// Represents the state of a transaction in the two-phase commit protocol.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionState {
    /// The transaction has been prepared but not yet committed or aborted.
    Prepared,
    /// The transaction has been committed.
    Committed,
    /// The transaction has been aborted.
    Aborted,
}

/// Represents a transaction commit that is replicated through OmniPaxos.
#[derive(Entry, Debug, Clone, Serialize, Deserialize)]
#[snapshot(ReplicatedCommitSnapshot)]
pub struct ReplicatedCommit {
    /// The globally unique identifier for this transaction.
    pub transaction_id: GlobalTransactionId,
    /// The set of changes to be applied by this transaction.
    pub write_set: HashMap<String, SerializableVersionedValue>,
    /// The current state of the transaction in the 2PC protocol.
    pub state: TransactionState,
    /// The read set of the transaction, used for validation during recovery.
    pub read_set: HashMap<String, u64>,
    /// The timestamp when this transaction was prepared.
    pub prepare_timestamp: u64,
    /// The timestamp when this transaction was committed or aborted.
    pub decision_timestamp: Option<u64>,
    /// The node that coordinated this transaction.
    pub coordinator_node: NodeId,
    /// The set of nodes that participated in this transaction.
    pub participant_nodes: Vec<NodeId>,
}

impl ReplicatedCommit {
    /// Creates a new ReplicatedCommit in the Prepared state.
    pub fn new_prepared(
        transaction_id: GlobalTransactionId,
        write_set: HashMap<String, SerializableVersionedValue>,
        read_set: HashMap<String, u64>,
        prepare_timestamp: u64,
        coordinator_node: NodeId,
        participant_nodes: Vec<NodeId>,
    ) -> Self {
        Self {
            transaction_id,
            write_set,
            state: TransactionState::Prepared,
            read_set,
            prepare_timestamp,
            decision_timestamp: None,
            coordinator_node,
            participant_nodes,
        }
    }
    
    /// Marks this transaction as committed.
    pub fn mark_committed(&mut self, decision_timestamp: u64) {
        self.state = TransactionState::Committed;
        self.decision_timestamp = Some(decision_timestamp);
    }
    
    /// Marks this transaction as aborted.
    pub fn mark_aborted(&mut self, decision_timestamp: u64) {
        self.state = TransactionState::Aborted;
        self.decision_timestamp = Some(decision_timestamp);
    }
    
    /// Returns whether this transaction is committed.
    pub fn is_committed(&self) -> bool {
        self.state == TransactionState::Committed
    }
    
    /// Returns whether this transaction is aborted.
    pub fn is_aborted(&self) -> bool {
        self.state == TransactionState::Aborted
    }
    
    /// Returns whether this transaction is prepared but not yet decided.
    pub fn is_prepared(&self) -> bool {
        self.state == TransactionState::Prepared
    }
}

/// Snapshot for ReplicatedCommit used by OmniPaxos.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedCommitSnapshot {
    /// Map of keys to their versions in the snapshot.
    pub snapshotted: HashMap<String, u64>,
    /// The last transaction ID in the snapshot.
    pub last_transaction_id: Option<GlobalTransactionId>,
}

impl Snapshot<ReplicatedCommit> for ReplicatedCommitSnapshot {
    fn create(entries: &[ReplicatedCommit]) -> Self {
        let mut snapshotted = HashMap::new();
        let mut last_transaction_id = None;
        
        for entry in entries {
            // Only include committed transactions in the snapshot
            if entry.state == TransactionState::Committed {
                for (key, value) in &entry.write_set {
                    snapshotted.insert(key.clone(), value.version());
                }
                last_transaction_id = Some(entry.transaction_id.clone());
            }
        }
        
        Self {
            snapshotted,
            last_transaction_id,
        }
    }
    
    fn merge(&mut self, other: ReplicatedCommitSnapshot) {
        // Merge the other snapshot's entries, keeping the higher version for each key
        for (key, version) in other.snapshotted {
            self.snapshotted
                .entry(key)
                .and_modify(|v| {
                    if version > *v {
                        *v = version;
                    }
                })
                .or_insert(version);
        }
        
        // Take the later transaction ID
        if let Some(other_id) = other.last_transaction_id {
            match &self.last_transaction_id {
                Some(self_id) => {
                    if other_id.timestamp > self_id.timestamp {
                        self.last_transaction_id = Some(other_id);
                    }
                },
                None => {
                    self.last_transaction_id = Some(other_id);
                }
            }
        }
    }
    
    fn use_snapshots() -> bool {
        // Enable snapshots for better performance with large logs
        true
    }
}

pub mod paxos_service {
    // Include the generated protobuf code here
    // The build script will generate this code.
    // We declare the module here so other parts of the distributed module can use it.
    // The actual content will be in target/debug/build/.../out/paxos_service.rs
    tonic::include_proto!("paxos_service");
}

/// Prelude of the distributed commits.
pub mod prelude {
    pub use super::*;
    pub use super::twopc::{
        TwoPhaseCommitManager, TwoPhaseCommitTransaction, TwoPhaseCommitPhase,
        ParticipantState, Participant,
    };
}
