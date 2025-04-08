use std::net::SocketAddr;
use crate::p2p::offer_reply::Offer;
use crate::p2p::peer::{Peer, PeerId};
use crate::p2p::peer_connector::{PeerConnectionCreator, PeerListenerCreator};
use crate::p2p::session::Session;
use crate::util::minecraft_connector::MinecraftConnector;
use crate::util::minecraft_listener::MinecraftListener;
use crate::util::parse_server::parse_server;
use crate::util::proxy_traffic::proxy_traffic;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument, Span};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;

mod p2p;
mod util;

#[derive(Parser)]
struct Cli {
    #[clap(short, long, help = "Identifier for this peer")]
    id: String,
    #[clap(
        short,
        long,
        default_value = "ws://127.0.0.1:5100",
        help = "Signaling server URL"
    )]
    signaling_server: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[clap(short, long, default_value = "127.0.0.1:3000")]
        minecraft_server: String,
    },
    Client {
        // Use 127.0.0.2 as this is less likely to be DNS filtered
        #[clap(short, long, default_value = "127.0.0.2:25565")]
        minecraft_adapter: String,
    },
}

struct App {
    session: Session,
}

#[tokio::main]
async fn main() -> Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_thread_names(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let peer = Peer {
        id: cli.id.to_string(),
    };

    info!(id = cli.id, "Starting jude as {}", cli.id);

    let session = Arc::new(Session::new(cli.signaling_server.to_string()).await?);

    match cli.command {
        Command::Server { minecraft_server } => {
            jude_server(cli.id, session, &minecraft_server).await
        }
        Command::Client { minecraft_adapter } => {
            jude_client(cli.id, session, &minecraft_adapter).await
        }
    }
}

#[tracing::instrument(name = "server", skip(session, minecraft_server))]
pub async fn jude_server(id: PeerId, session: Arc<Session>, minecraft_server: &str) -> Result<()> {
    info!(session.server, minecraft_server, "Starting jude server");

    let session = session;
    let mut listener = session.listener(id.to_string()).await?;

    while let Some(offer) = listener.receive().await {
        let session = Arc::clone(&session);
        let minecraft_server = minecraft_server.to_string();
        tokio::spawn(async move {
            let result = handle_offer(offer, &session, &minecraft_server).await;
            if let Err(e) = result {
                error!(error = ?e, "Server connection failed");
            }
        });
    }

    Ok(())
}

async fn handle_offer(offer: Offer, session: &Session, minecraft_server: &str) -> Result<()> {
    let mut peer_connection = session.accept(offer).await?;

    let data_channel = peer_connection.default.take().unwrap().detach().await?;

    let minecraft_stream = MinecraftConnector::connect(minecraft_server).await?;

    let cancellation_token = CancellationToken::new();
    proxy_traffic(data_channel, minecraft_stream, cancellation_token.clone()).await?;
    cancellation_token.cancel();

    Ok(())
}

#[tracing::instrument(name = "client", skip(session, minecraft_adapter))]
async fn jude_client(id: PeerId, session: Arc<Session>, minecraft_adapter: &str) -> Result<()> {
    info!(session.server, minecraft_adapter, "Starting client proxy");
    let listener = MinecraftListener::bind(minecraft_adapter)
        .await
        .context("Failed to bind Minecraft listener")?;

    // Main client acceptance loop with proper error containment
    loop {
        // Use tokio::select! to handle cancellation/cleanup
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, addr)) => {
                        let peer_id = id.clone();
                        let session_clone = session.clone();

                        tokio::spawn(async move {
                                let result = handle_connection(
                                    stream,
                                    addr,
                                    peer_id,
                                    &session_clone
                                ).await;

                                if let Err(e) = result {
                                    error!(error = ?e, "Client connection failed");
                                }
                            }.instrument(info_span!("client_session", client = ?addr)));
                    }
                    Err(e) => {
                        error!(error = ?e, "Accept error, continuing listener");
                        continue;
                    }
                }
            }
            // TODO graceful shutdown handling here
        }
    }
}

#[tracing::instrument(
    name = "handle_connection",
    skip(stream, session, peer_id),
    fields(client = ?addr, server = tracing::field::Empty)
)]
async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    peer_id: PeerId,
    session: &Session,
) -> Result<()> {
    let peer = Peer {
        id: peer_id.to_string(),
    };

    let server = parse_server(&mut stream)
        .await
        .context("Failed to parse Minecraft server")?;

    Span::current().record("server", &server);

    let mut connection = session
        .connect(peer_id, server)
        .await
        .context("Failed to establish WebRTC connection")?;

    /*let data_channel = connection.open_detached_channel("minecraft".to_string())
    .await
    .context("Failed to create data channel")?;*/
    let data_channel = connection.default.take().unwrap().detach().await?;

    let cancel_token = CancellationToken::new();
    proxy_traffic(data_channel, stream, cancel_token.clone())
        .await
        .context("Proxy traffic failed")?;

    cancel_token.cancel();

    Ok(())
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
