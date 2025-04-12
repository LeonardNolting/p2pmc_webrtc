use std::sync::Arc;
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::session::Session;
use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use crate::core::p2p::offer_reply::Offer;
use crate::core::p2p::peer_connector::{PeerConnectionCreator, PeerListenerCreator};
use crate::util::minecraft_connector::MinecraftConnector;
use crate::util::proxy_traffic::proxy_traffic;

#[tracing::instrument(name = "server", skip(session, minecraft_server))]
pub async fn jude_server(id: PeerId, session: &Arc<Session>, minecraft_server: &str) -> Result<()> {    
    info!(session.server, minecraft_server, "Starting jude server");
    let session = Arc::clone(session);

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

    let data_channel = peer_connection.primary.take().unwrap().detach().await?;

    let minecraft_stream = MinecraftConnector::connect(minecraft_server).await?;

    let cancellation_token = CancellationToken::new();
    proxy_traffic(data_channel, minecraft_stream, cancellation_token.clone()).await?;
    cancellation_token.cancel();

    Ok(())
}