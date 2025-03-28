use crate::p2p::offer_reply::Offer;
use crate::p2p::peer::PeerId;
use crate::p2p::peer_connection::{PeerConnection, UnacceptedPeerConnection};
use anyhow::Result;
use tokio::sync::mpsc;
use crate::p2p::signaling_connection::SignalingConnection;

pub trait PeerConnector<S: SignalingConnection> {
    fn get_signaling_connection(&self) -> &S;
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    async fn connect(&self, id: PeerId, to: PeerId) -> Result<PeerConnection> {
        PeerConnection::connect::<S>(id, to, self.get_signaling_connection()).await
    }
    async fn accept(&mut self) -> Option<Offer> {
        self.get_connection_receiver().recv().await
    }
}