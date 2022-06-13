use std::{net::SocketAddr, collections::HashMap, error::Error};
use tokio::{net::TcpListener, sync::mpsc::{self, Sender, Receiver}};
use crate::communication::control_plane::notifications::{HeadNotification, WorkerNotification};
use super::worker_node::WorkerNode;
use rand::Rng;

pub struct LeaderNode;

impl LeaderNode {
    pub async fn start_leader(address: SocketAddr) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(address).await?;
        let mut state: HashMap<usize, WorkerNode> = HashMap::new();
        let (tx_leader, mut rx_leader): (Sender<HeadNotification>, Receiver<HeadNotification>) = mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                let (stream, addr) = listener.accept().await.unwrap();
                println!("New connection: {}", stream.peer_addr().unwrap());

                // Sender that all Workers have a copy of so they can communicate with the Leader
                let mut tx_leader = tx_leader.clone();
                let id = rand::thread_rng().gen_range(0, 1000);
                tokio::spawn(async move {
                    let new_worker = WorkerNode::attach_worker(id, stream, addr, &mut tx_leader).await;
                    // add to state
                });
            }
        });

        while let Some(message) = rx_leader.recv().await {
            println!("Leader received: {:?}", message);
        }
        Ok(())
    }
}