use std::net::SocketAddr;
use tokio::{net::TcpStream, sync::mpsc::{self, Sender, Receiver}};
use tokio_util::codec::{Framed};
use futures_util::stream::StreamExt;

use crate::communication::control_plane::{notifications::{WorkerNotification, HeadNotification}, codecs::ControlPlaneCodec};

pub struct WorkerNode {
    tx_leader: Sender<HeadNotification>,
}

impl WorkerNode {
    pub fn new(tx_leader: Sender<HeadNotification>) -> Self {
        WorkerNode {
            tx_leader,
        }
    }

    async fn handle_messages(
        framed: &mut Framed<TcpStream, ControlPlaneCodec<WorkerNotification, HeadNotification>>,
        message: WorkerNotification,
    ) {
        println!("Got message {:?}", message);
    }

    pub async fn attach_worker(
        id: usize,
        stream: TcpStream,
        address: SocketAddr,
        tx_leader: &mut Sender<HeadNotification>,
    ) -> Self {
        let mut framed = Framed::new(stream, ControlPlaneCodec::<WorkerNotification, HeadNotification>::new());
        // let (split_sink, split_stream) = framed.split();
        
        // Channel to send WorkerNotifiations from Worker to Leader
        let (tx_worker, rx_worker): (Sender<WorkerNotification>, Receiver<WorkerNotification>) = mpsc::channel(32);

        let worker_node = WorkerNode::new(tx_leader.clone());
        worker_node.tx_leader.send(HeadNotification::Ready(id)).await.unwrap();

        return worker_node;
    }
}