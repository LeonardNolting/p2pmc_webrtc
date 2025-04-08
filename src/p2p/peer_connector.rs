use crate::p2p::peer::PeerId;
use crate::p2p::peer_connection::PeerConnection;
use crate::p2p::session::PeerListener;
use crate::p2p::signaling_connection::SignalingConnection;
use anyhow::Result;
use crate::p2p::offer_reply::Offer;

pub trait PeerConnectionCreator<S: SignalingConnection> {
    fn get_signaling_connection(&self) -> &S;
    async fn connect(&self, id: PeerId, to: PeerId) -> Result<PeerConnection> {
        PeerConnection::connect(id, to, self.get_signaling_connection()).await
    }

    async fn accept(&self, offer: Offer) -> Result<PeerConnection> {
        PeerConnection::accept(offer, self.get_signaling_connection()).await
    }
}

pub trait PeerListenerCreator<S: SignalingConnection> {
    async fn listen(&self, id: PeerId) -> PeerListener;
}