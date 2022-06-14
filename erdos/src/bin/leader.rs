use erdos::node::leader_node::LeaderNode;

#[tokio::main]
async fn main() {
    let address = "127.0.0.1:4444".parse().unwrap();

    let mut leader = LeaderNode::new();
    if let Err(error) = leader.start_leader(address).await {
        println!("Received error: {:?}", error);
    }
}
