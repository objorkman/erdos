use std::{net::SocketAddr, collections::HashMap};
use futures::{StreamExt, SinkExt, stream::{SplitSink, SplitStream}};
use tokio::{
    net::{TcpStream, TcpListener},
    sync::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver},
        Mutex,
    },
};
use crate::{Uuid, communication::{control_plane::codecs::ControlPlaneCodec, CodecError, CommunicationError}};
use crate::communication::control_plane::notifications::{ControlPlaneNotification};
use tokio_util::codec::Framed;

// Unique worker id
pub type WorkerId = Uuid;

pub struct WorkerConnection {
    split_sink: SplitSink<Framed<TcpStream, ControlPlaneCodec<ControlPlaneNotification>>, ControlPlaneNotification>,
    split_stream: SplitStream<Framed<TcpStream, ControlPlaneCodec<ControlPlaneNotification>>>,
}

impl WorkerConnection {
    pub fn new(worker_connection_stream: TcpStream) -> Self {
        let framed = Framed::new(worker_connection_stream, ControlPlaneCodec::<ControlPlaneNotification>::new());
        let (split_sink, split_stream) = framed.split();
        Self {
            split_sink,
            split_stream,
        }
    }

    pub async fn send(&mut self, message: ControlPlaneNotification) -> Result<(), CodecError> {
        Ok(self.split_sink.send(message).await?)
    }
}

pub struct LeaderNode {
    node_id_to_connection: HashMap<WorkerId, WorkerConnection>,
}

impl LeaderNode {
    pub fn new() -> Self {
        Self {
            node_id_to_connection: HashMap::new(),
        }
    }

    pub async fn start_leader(&mut self, address: SocketAddr) -> Result<(), CommunicationError> {
        let listener = TcpListener::bind(address).await?;
        self.await_worker_connection(listener).await?;
        Ok(())
    }

    async fn await_worker_connection(&mut self, listener: TcpListener) -> Result<(), CommunicationError>{
        loop {
            let (stream, address) = listener.accept().await.unwrap();
            let worker_id = WorkerId::new_deterministic();
            println!("Received connection from address: {} and assigned worker ID: {}", address, worker_id);
            self.node_id_to_connection.insert(worker_id, WorkerConnection::new(stream));
            
            let worker_connection = self.node_id_to_connection.get_mut(&worker_id).unwrap();
            worker_connection.send(ControlPlaneNotification::Ready(worker_id)).await?;

            // Channel to send LeaderNotifiations from Worker to Leader
            let (tx_leader, rx_leader): (Sender<ControlPlaneNotification>, Receiver<ControlPlaneNotification>) = mpsc::unbounded_channel();
            tx_leader.send(ControlPlaneNotification::Testing).await.unwrap();
        }
    }
}