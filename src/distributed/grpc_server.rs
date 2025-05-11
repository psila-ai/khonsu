use bincode;
use crossbeam_channel::Sender;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tonic::{transport::Server, Request, Response, Status};

use crate::distributed::paxos_service::paxos_service_server::{
    PaxosService, PaxosServiceServer,
};
use crate::distributed::paxos_service::ConsensusMessage;
use crate::distributed::paxos_service::SendMessageResponse;
use crate::distributed::ReplicatedCommit;

pub struct KhonsuOmniPaxosService {
    // Sender channel to the OmniPaxos event loop thread.
    // Received gRPC messages will be sent through this channel.
    omni_paxos_sender: Sender<Message<ReplicatedCommit>>,
}

impl KhonsuOmniPaxosService {
    pub fn new(
        omni_paxos_sender: Sender<Message<ReplicatedCommit>>,
        _node_id: NodeId, // Keep parameter but ignore it
    ) -> Self {
        Self { 
            omni_paxos_sender,
        }
    }
}

#[tonic::async_trait]
impl PaxosService for KhonsuOmniPaxosService {
    async fn send_message(
        &self,
        request: Request<ConsensusMessage>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let message_proto = request.into_inner();

        // Deserialize the OmniPaxos message payload.
        let message: Message<ReplicatedCommit> =
            bincode::deserialize(&message_proto.message_payload).map_err(|e| {
                Status::internal(format!("Failed to deserialize OmniPaxos message: {:?}", e))
            })?;

        // Send the message to the OmniPaxos event loop thread.
        if let Err(e) = self.omni_paxos_sender.send(message) {
            eprintln!(
                "Error sending received message to OmniPaxos thread: {:?}",
                e
            );
            return Err(Status::internal(format!(
                "Failed to send message to OmniPaxos thread: {:?}",
                e
            )));
        }

        // Return a success response.
        let reply = SendMessageResponse { success: true };
        Ok(Response::new(reply))
    }
}

/// Starts the gRPC server for OmniPaxos communication.
///
/// # Arguments
///
/// * `addr` - The socket address to bind the server to.
/// * `omni_paxos_sender` - Sender channel to the OmniPaxos event loop thread.
/// * `node_id` - The ID of the local node.
/// * `runtime` - The Tokio runtime for async operations.
pub fn start_grpc_server(
    addr: SocketAddr,
    omni_paxos_sender: Sender<Message<ReplicatedCommit>>,
    node_id: NodeId,
    runtime: Arc<Runtime>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = KhonsuOmniPaxosService::new(omni_paxos_sender, node_id);

    println!("gRPC server listening on {}", addr);

    // Run the server in the provided runtime
    runtime.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(PaxosServiceServer::new(service))
            .serve(addr)
            .await
        {
            eprintln!("gRPC server error: {}", e);
        }
    });

    Ok(())
}
