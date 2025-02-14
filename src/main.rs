use crate::client::client;
use crate::server::server;

mod client;
mod p2p_helper;
mod server;

#[tokio::main]
async fn main() {
    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";

    if is_client {
        println!("Running as client");
        client().await.unwrap();
    } else {
        println!("Running as server");
        server().await.unwrap();
    }
}
