use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, BytesCodec};
use erdos::node::leader_node::LeaderNode;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = "127.0.0.1:4444".parse().unwrap();
    LeaderNode::start_leader(address).await?;
    Ok(())
}
