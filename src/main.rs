use crate::client::start_client_proxy;
use crate::server::start_server_proxy;

mod client;
mod p2p_helper;
mod server;
mod general;
mod reply_manager;
mod log_on_drop;
mod parse_server;

// pub const SIGNALING_SERVER: &str = "http://34.75.203.169:5100";
pub const SIGNALING_SERVER: &str = "ws://127.0.0.1:5100";

#[tokio::main]
async fn main() {
    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";
    let id = std::env::args().nth(2).expect("Provide an ID");

    if is_client {
        let port = std::env::args().nth(3).unwrap_or("25565".to_string()).parse::<u16>().expect("Port must be a number");
        start_client_proxy(SIGNALING_SERVER, id.as_str(), port).await;
    } else {
        let port = std::env::args().nth(3).expect("Provide a port on which a Minecraft server runs").parse::<u16>().expect("Port must be a number");
        start_server_proxy(SIGNALING_SERVER, id.as_str(), port).await;
    }
}
