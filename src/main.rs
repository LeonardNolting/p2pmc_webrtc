use crate::client::start_client_proxy;
use crate::server::start_server_proxy;

mod client;
mod p2p_helper;
mod server;
mod general;
mod reply_manager;
mod log_on_drop;

#[tokio::main]
async fn main() {
    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";

    if is_client {
        start_client_proxy("http://localhost:5100", "TESTID1").await;
    } else {
        start_server_proxy("http://localhost:5100", "TESTID2").await;
    }
}
