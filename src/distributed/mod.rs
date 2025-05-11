// This module will contain the distributed commit implementation.

pub mod dist_config;
pub mod grpc_server; // Module for the gRPC server implementation
pub mod manager; // Module for the DistributedCommitManager
pub mod network; // Module for OmniPaxos network implementation
pub mod storage; // Module for OmniPaxos storage backend
pub mod twopc; // Module for the Distributed Config

use crate::data_store::versioned_value::VersionedValue;
use omnipaxos::{macros::Entry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Entry, Debug, Clone, Serialize, Deserialize)]
#[snapshot(ReplicatedCommitSnapshot)]
pub struct ReplicatedCommit {
    pub transaction_id: GlobalTransactionId,
    pub write_set: HashMap<String, VersionedValue>, // Assuming write set is a map of String keys to VersionedValue
                                                    // Add other necessary information for commit/rollback if needed
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedCommitSnapshot {
    snapshotted: HashMap<String, u64>,
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
}
