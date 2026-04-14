use crate::core::p2p::peer::{Peer, PeerId};
use crate::core::p2p::peer_connector::PeerConnectionCreator;
use crate::core::p2p::session::Session;
use crate::dht::lookup_iroh_mapping;
use crate::dumbpipe::connect_tcp;
use crate::util::minecraft_listener::MinecraftListener;
use crate::util::parse_server::parse_server;
use crate::util::proxy_traffic::proxy_traffic;
use anyhow::Context;
use cancellable::cancellable;
use flutter_rust_bridge::frb;
use pkarr::Client;
use std::net::SocketAddr;
use tokio::net::TcpStream;
pub use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument, Span};

/// Generation of flutter_rust_bridge bindings only works when CancellationToken is here
/// Don't ask why
/// tested in: /src/lib.rs, /src/cancellation_token.rs, /src/core/proxies/cancellation_token.rs; always with pub modules etc.; works only here?
/// Also tested adding a mirrored struct with #[frb(mirror(tokio_util::sync::CancellationToken))]
#[frb(external)]
impl CancellationToken {
    #[frb(sync)]
    pub fn new() -> CancellationToken {}
    #[frb(sync)]
    pub fn cancel(&self) {}
    #[frb(sync)]
    pub fn clone(&self) -> Self {}
}

#[tracing::instrument(name = "client", skip(minecraft_adapter))]
#[cancellable]
pub async fn jude_client(id: PeerId, minecraft_adapter: String) -> anyhow::Result<()> {
    info!(minecraft_adapter, "Starting client proxy");
    let listener = MinecraftListener::bind(minecraft_adapter)
        .await
        .context("Failed to bind Minecraft listener")?;

    // Main client acceptance loop with proper error containment
    loop {
        while let Ok((stream, addr)) = listener.accept().await {
            let peer_id = id.clone();

            tokio::spawn(
                async move {
                    let result = handle_connection(stream, addr, peer_id).await;

                    if let Err(e) = result {
                        error!(error = ?e, "Client connection failed");
                    }
                }
                .instrument(info_span!("client_session", client = ?addr)),
            );
        }
    }
}

#[tracing::instrument(
    name = "handle_connection",
    skip(stream, peer_id),
    fields(client = ?addr, server = tracing::field::Empty)
)]
async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    peer_id: PeerId,
) -> anyhow::Result<()> {
    let peer = Peer {
        id: peer_id.to_string(),
    };

    let server = parse_server(&mut stream)
        .await
        .context("Failed to parse Minecraft server")?;

    Span::current().record("server", &server);

    let cancel_token = CancellationToken::new();
    let ticket = lookup_iroh_mapping(Client::builder().build()?, server)
        .await
        .expect("Failed to lookup ticket")
        .expect("Ticket is not published");
    info!("CONNECTION STARTED {}, ticket={}", addr, ticket);

    connect_tcp(cancel_token.clone(), None, , ticket).await?;

    error!("CONNECTION STOPPED {}", addr);

    cancel_token.cancel();

    Ok(())
}
