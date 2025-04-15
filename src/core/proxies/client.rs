use crate::core::p2p::peer::{Peer, PeerId};
use crate::core::p2p::peer_connector::PeerConnectionCreator;
use crate::core::p2p::session::Session;
use crate::util::minecraft_listener::MinecraftListener;
use crate::util::parse_server::parse_server;
use crate::util::proxy_traffic::proxy_traffic;
use anyhow::Context;
use cancellable::cancellable;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, Instrument, Span};

#[tracing::instrument(name = "client", skip(session, minecraft_adapter))]
#[cancellable]
pub async fn jude_client(
    id: PeerId,
    session: Session,
    minecraft_adapter: String,
) -> anyhow::Result<()> {
    info!(session.server, minecraft_adapter, "Starting client proxy");
    let session = session.clone();
    let listener = MinecraftListener::bind(minecraft_adapter)
        .await
        .context("Failed to bind Minecraft listener")?;

    // Main client acceptance loop with proper error containment
    loop {
        while let Ok((stream, addr)) = listener.accept().await {
            let peer_id = id.clone();
            let session_clone = session.clone();

            tokio::spawn(
                async move {
                    let result = handle_connection(stream, addr, peer_id, &session_clone).await;

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
    skip(stream, session, peer_id),
    fields(client = ?addr, server = tracing::field::Empty)
)]
async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    peer_id: PeerId,
    session: &Session,
) -> anyhow::Result<()> {
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
    let data_channel = connection.primary.take().unwrap().detach().await?;

    let cancel_token = CancellationToken::new();
    proxy_traffic(data_channel, stream, cancel_token.clone())
        .await
        .context("Proxy traffic failed")?;

    cancel_token.cancel();

    Ok(())
}
