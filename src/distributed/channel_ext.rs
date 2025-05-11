use crossbeam_channel as channel;
use omnipaxos::util::NodeId;

use crate::distributed::ReplicatedCommit;

/// Extension trait for `channel::Sender<ReplicatedCommit>` to add node_id method.
pub trait SenderExt {
    /// Returns the node ID associated with this sender.
    fn node_id(&self) -> NodeId;
}

/// A wrapper around `channel::Sender<ReplicatedCommit>` that includes the node ID.
pub struct NodeSender {
    /// The underlying sender.
    pub sender: channel::Sender<ReplicatedCommit>,
    /// The node ID.
    pub node_id: NodeId,
}

impl NodeSender {
    /// Creates a new NodeSender.
    pub fn new(sender: channel::Sender<ReplicatedCommit>, node_id: NodeId) -> Self {
        Self { sender, node_id }
    }

    /// Sends a message through the channel.
    pub fn send(&self, msg: ReplicatedCommit) -> Result<(), channel::SendError<ReplicatedCommit>> {
        self.sender.send(msg)
    }
}

impl SenderExt for NodeSender {
    fn node_id(&self) -> NodeId {
        self.node_id
    }
}

// We can't implement methods directly on foreign types, so we use the trait instead
