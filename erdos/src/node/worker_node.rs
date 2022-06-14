use std::{net::SocketAddr, error::Error};
use tokio::{net::{TcpStream, TcpListener}, sync::mpsc::{self, Sender, Receiver}};
use tokio_util::codec::{Framed};
use futures_util::stream::StreamExt;

use crate::communication::control_plane::{notifications::{ControlPlaneNotification}, codecs::ControlPlaneCodec};

pub struct WorkerNode {
    leader_address: SocketAddr,
}

impl WorkerNode {
    pub async fn new(leader_address: SocketAddr) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            leader_address,
        })
    }

    pub async fn connect_to_leader(&self) -> Result<(), Box<dyn Error>> {
       let stream = TcpStream::connect(self.leader_address).await?;
        // Reads LeaderNotifications and sends WorkerNotifications
        let framed = Framed::new(stream, ControlPlaneCodec::<ControlPlaneNotification>::new());
        let (split_sink, mut split_stream) = framed.split();
        
        match split_stream.next().await {
            Some(Ok(control_plane_message)) => {
                match control_plane_message {
                    ControlPlaneNotification::Ready(worker_id) => {
                        println!("Received the WorkerId: {} from the Leader.", worker_id)
                    }
                } 
            }
            _ => {
                
            }
        }

        // // Channel to send LeaderNotifiations from Worker to Leader
        // let (tx_leader, rx_leader): (Sender<LeaderNotification>, Receiver<LeaderNotification>) = mpsc::unbounded_channel();

        // tx_leader.send(LeaderNotification::Ready()).await.unwrap();
        Ok(())
    }
}