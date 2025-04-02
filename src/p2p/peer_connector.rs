use crate::p2p::offer_reply::Offer;
use crate::p2p::peer::PeerId;
use crate::p2p::peer_connection::{PeerConnection, UnacceptedPeerConnection};
use anyhow::Result;
use tokio::sync::mpsc;
use crate::p2p::signaling_connection::SignalingConnection;

/// Provides .connect()
pub trait PeerConnector<S: SignalingConnection> {
    fn get_signaling_connection(&self) -> &S;
    async fn connect(&self, id: PeerId, to: PeerId) -> Result<PeerConnection> {
        PeerConnection::connect::<S>(id, to, self.get_signaling_connection()).await
    }
}