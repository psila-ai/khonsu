use omnipaxos::util::NodeId;
use omnipaxos::ClusterConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KhonsuDistConfig {
    pub node_id: NodeId,
    pub cluster_config: ClusterConfig,
    pub peer_addrs: HashMap<NodeId, String>,
    pub storage_path: PathBuf,
}
