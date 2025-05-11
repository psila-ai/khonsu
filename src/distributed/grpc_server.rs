use bincode;
use crossbeam_channel::Sender;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};

use crate::distributed::paxos_service::paxos_service_server::{
    PaxosService, PaxosServiceServer,
};
use crate::distributed::paxos_service::ConsensusMessage;
use crate::distributed::ReplicatedCommit; // Import the replicated data type // Alias

pub struct KhonsuOmniPaxosService {
    // Sender channel to the OmniPaxos event loop thread.
    // Received gRPC messages will be sent through this channel.
    omni_paxos_sender: Sender<Message<ReplicatedCommit>>, // Using tokio mpsc for async context
                                                                // TODO: Need a way to get the local node ID if needed in the service implementation.
}

impl KhonsuOmniPaxosService {
    pub fn new(omni_paxos_sender: Sender<Message<ReplicatedCommit>>) -> Self {
        Self { omni_paxos_sender }
    }
}

#[tonic::async_trait]
impl PaxosService for KhonsuOmniPaxosService {
    async fn send_message(
        &self,
        request: Request<ConsensusMessage>,
    ) -> Result<Response<super::paxos_service::SendMessageResponse>, Status> {
        let message_proto = request.into_inner();

        // Deserialize the OmniPaxos message payload.
        let message: Message<ReplicatedCommit> =
            bincode::deserialize(&message_proto.message_payload).map_err(|e| {
                Status::internal(format!("Failed to deserialize OmniPaxos message: {:?}", e))
            })?; // TODO: Handle errors

        // Send the message to the OmniPaxos event loop thread.
        // This is an async send.
        if let Err(e) = self.omni_paxos_sender.send(message).await {
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
        let reply = super::paxos_service::SendMessageResponse { success: true };
        Ok(Response::new(reply))
    }
}

/// Starts the gRPC server for OmniPaxos communication.
///
/// # Arguments
///
/// * `addr` - The socket address to bind the server to.
/// * `omni_paxos_sender` - Sender channel to the OmniPaxos event loop thread.
pub async fn start_grpc_server(
    addr: SocketAddr,
    omni_paxos_sender: Sender<Message<ReplicatedCommit>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = KhonsuOmniPaxosService::new(omni_paxos_sender);

    println!("gRPC server listening on {}", addr);

    Server::builder()
        .add_service(PaxosServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

// TODO: Integrate this server startup into the DistributedCommitManager.
// TODO: Handle graceful shutdown of the server.
