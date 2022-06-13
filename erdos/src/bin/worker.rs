use tokio::net::TcpStream;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let address: SocketAddr = "127.0.0.1:4444".parse().unwrap();
    TcpStream::connect(address).await.unwrap();
}
