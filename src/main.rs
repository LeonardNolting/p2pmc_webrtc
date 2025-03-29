use std::sync::Arc;
use crate::p2p::peer::Peer;
use crate::p2p::peer_connection::PeerConnection;
use crate::p2p::peer_connector::PeerConnector;
use crate::p2p::session::Session;
use crate::util::proxy_traffic::proxy_traffic;
use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{info, info_span, Instrument, Span};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;
use crate::util::minecraft_connector::MinecraftConnector;
use crate::util::minecraft_listener::MinecraftListener;
use crate::util::parse_server::parse_server;

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
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
        #[clap(long, default_value = "localhost:3000")]
        // #[clap(long, default_value = "serveo.net:3001")]
        minecraft_server: String,
    },
    Client {
        #[clap(long, default_value = "client")]
        id: String,
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
        #[clap(long, default_value = "127.0.0.2:25565")]
        minecraft_adapter: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_thread_names(true)
        // .with_span_events(FmtSpan::NEW)
        // Display source code file paths
        // .with_file(true)
        // Display source code line numbers
        // .with_line_number(true)
        // Display the thread ID an event was recorded on
        // .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;
    
    let cli = Cli::parse();
    match cli.command {
        Command::Server {
            id,
            signaling_server,
            minecraft_server,
        } => run_server_proxy(&signaling_server, &id, &minecraft_server).await,
        Command::Client {
            id,
            signaling_server,
            minecraft_adapter,
        } => run_client_proxy(&signaling_server, &id, &minecraft_adapter).await,
    }
}

#[tracing::instrument(name = "server", skip(signaling_host, minecraft_server))]
async fn run_server_proxy(signaling_host: &str, id: &str, minecraft_server: &str) -> Result<()> {
    info!(signaling_host, minecraft_server, "Starting server proxy");
    let peer = Peer { id: id.to_string() };

    let mut session = Session::new(signaling_host.to_string()).await?;

    session.register(peer.id.clone()).await?;

    while let Some(offer) = session.accept().await {
        let peer_connection = PeerConnection::accept(offer, &session).await?;

        let data_channel = peer_connection
            .accept_channel_detached("minecraft".to_string())
            .await.await;

        let minecraft_stream = MinecraftConnector::connect(minecraft_server).await?;

        proxy_traffic(data_channel, minecraft_stream).await?;
    }

    Ok(())
}

#[tracing::instrument(name = "client", skip(signaling_host, minecraft_adapter))]
async fn run_client_proxy(signaling_host: &str, id: &str, minecraft_adapter: &str) -> Result<()> {
    info!(signaling_host, minecraft_adapter, "Starting client proxy");
    let peer = Peer { id: id.to_string() };

    let session = Arc::new(Session::new(signaling_host.to_string()).await?);

    session.clone().register(peer.id.clone()).await?;

    let listener = MinecraftListener::bind(minecraft_adapter).await?;

    loop {
        let (mut stream, _addr) = listener.accept().await?;

        let id = id.to_string();
        let session = session.clone();
        
        tokio::spawn(async move {
            let server = parse_server(&mut stream).await.unwrap();
            Span::current().record("server", &server);
            
            let connection = session
                .connect(id, server)
                .await.expect("Error handling client connection");

            let data_channel = connection.open_detached_channel("minecraft".to_string()).await.unwrap();

            proxy_traffic(data_channel, stream).await.unwrap();
        }.instrument(info_span!("minecraft_client_adapter_connection", server = tracing::field::Empty)));
    }
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
