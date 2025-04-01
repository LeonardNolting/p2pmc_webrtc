use std::sync::Arc;
use crate::p2p::peer::Peer;
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
    #[clap(short, long, help = "Identifier for this peer")]
    id: String,
    #[clap(short, long, default_value = "ws://34.75.203.169:5100", help = "Signaling server URL")]
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

    let peer = Peer { id: cli.id.to_string() };
    
    info!(id = cli.id, "Starting jude as {}", cli.id);

    let session = Session::new(cli.signaling_server.to_string()).await?;
    
    let app = App { session };
    
    match cli.command {
        Command::Server { minecraft_server } => {
            jude_server(&cli.id, &app.session, &minecraft_server).await
        },
        Command::Client { minecraft_adapter } => {
            jude_client(&cli.id, &app.session, &minecraft_adapter).await
        },
    }
}

#[tracing::instrument(name = "server", skip(session, minecraft_server))]
pub async fn jude_server(id: &str, session: &Session, minecraft_server: &str) -> Result<()> {
    info!(session.server, minecraft_server, "Starting jude server");

    let mut instance = session.register(id.to_string()).await?;

    while let Some(offer) = instance.receive().await {
        // Accepts all connections, no whitelist or blacklist
        // this would be different for a closed round, for example: you only accept connections from your 15 round mates
        let peer_connection = instance.accept(offer).await?;

        // TODO race condition: connecting peer could start minecraft data channel before we're awaiting it here (see double await)
        let data_channel = peer_connection
            .accept_channel_detached("minecraft".to_string())
            .await.await;

        let minecraft_stream = MinecraftConnector::connect(minecraft_server).await?;

        proxy_traffic(data_channel, minecraft_stream).await?;
    }

    Ok(())
}

#[tracing::instrument(name = "client", skip(session, minecraft_adapter))]
async fn jude_client(id: &str, session: &Session, minecraft_adapter: &str) -> Result<()> {
    info!(session.server, minecraft_adapter, "Starting client proxy");
    let peer = Peer { id: id.to_string() };

    let instance = Arc::new(session.register(peer.id.clone()).await?);

    let listener = MinecraftListener::bind(minecraft_adapter).await?;

    while let Ok((mut stream, _addr)) = listener.accept().await {
        let instance = instance.clone();

        tokio::spawn(async move {
            let server = parse_server(&mut stream).await.unwrap();
            Span::current().record("server", &server);

            let connection = instance
                .connect(server)
                .await.expect("Error handling client connection");

            let data_channel = connection.open_detached_channel("minecraft".to_string()).await.unwrap();

            proxy_traffic(data_channel, stream).await.unwrap();
        }.instrument(info_span!("minecraft_client_adapter_connection", server = tracing::field::Empty)));
    }
    
    Ok(())
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
