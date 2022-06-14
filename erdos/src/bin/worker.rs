use erdos::node::worker_node::WorkerNode;
use tokio::net::TcpStream;
use std::{net::SocketAddr, error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = "127.0.0.1:4444".parse().unwrap();
    if let Ok(worker) = WorkerNode::new(address).await {
        worker.connect_to_leader().await?;
    }
    Ok(())
}
