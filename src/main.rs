use std::sync::Arc;

use crate::p2p::peer::Peer;
use crate::p2p::peer_connection::PeerConnection;
use crate::p2p::peer_connector::PeerConnector;
use crate::p2p::session::Session;
use crate::util::proxy_traffic::proxy_traffic;
use anyhow::Result;
use clap::{Parser, Subcommand};
use util::minecraft_connections::{connect_to_local_server, listen_for_minecraft_client_connections};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;

mod util;
mod p2p;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[clap(long, default_value = "testserver")]
        id: String,
        // #[clap(long, default_value = "localhost:3000")]
        #[clap(long, default_value = "serveo.net:3001")]
        minecraft_server: String,
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
    },
    Client {
        #[clap(long, default_value = "client")]
        id: String,
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Server {
            id,
            minecraft_server,
            signaling_server,
        } => run_server_proxy(&signaling_server, &id, &minecraft_server).await,
        Command::Client {
            id,
            signaling_server,
        } => run_client_proxy(&signaling_server, &id).await,
    }
}

async fn run_server_proxy(signaling_host: &str, id: &str, minecraft_server: &str) -> Result<()> {
    let peer = Peer { id: id.to_string() };

    let mut session = Session::new(signaling_host.to_string()).await?;

    session.register(peer.id.clone()).await?;

    while let Some(offer) = session.accept().await {
        let peer_connection = PeerConnection::accept(offer, &session).await?;

        let data_channel = peer_connection
            .open_detached_channel("minecraft".to_string())
            .await?;

        let minecraft_stream = connect_to_local_server(minecraft_server).await;

        proxy_traffic(data_channel, minecraft_stream).await?;
    }

    Ok(())
}

async fn run_client_proxy(signaling_host: &str, id: &str) -> Result<()> {
    let peer = Peer { id: id.to_string() };

    let session = Arc::new(Session::new(signaling_host.to_string()).await?);

    session.clone().register(peer.id.clone()).await?;

    /* tokio::spawn(async move {
        while let Some(event) = receiver.next_event().await {
            if let Ok(SignalingEvent::Reply(reply)) = event {
                reply_manager_clone
                    .handle_response(reply.number, reply)
                    .await;
            } else {
                eprintln!("Unexpected message type: {}", offer_reply.r#type);
            }
        }
        eprintln!("Disconnected from signaling server");
    }); */

    listen_for_minecraft_client_connections("127.0.0.1:25565", {
        let session = session.clone();
        move |stream, _| {
            let session = session.clone();
            async move {
                if let Err(e) = session
                    .connect("client".to_string(), "testserver".to_string())
                    .await
                {
                    eprintln!("Error handling client connection: {}", e);
                }
            }
        }
    })
    .await;

    Ok(())
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
