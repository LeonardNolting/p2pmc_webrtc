use crate::core::p2p::peer::PeerId;
use crate::core::p2p::peer_connection::PeerConnection;
use crate::core::p2p::session::PeerListener;
use crate::core::p2p::signaling_connection::SignalingConnection;
use anyhow::Result;
use crate::core::p2p::offer_reply::Offer;

pub(crate) trait PeerConnectionCreator<S: SignalingConnection> {
    fn get_signaling_connection(&self) -> &S;
    async fn connect(&self, id: PeerId, to: PeerId) -> Result<PeerConnection> {
        let signaling_connection = self.get_signaling_connection();
        signaling_connection.register(id.clone()).await?;
        PeerConnection::connect(id, to, signaling_connection).await
    }

    async fn accept(&self, offer: Offer) -> Result<PeerConnection> {
        PeerConnection::accept(offer, self.get_signaling_connection()).await
    }
}

pub(crate) trait PeerListenerCreator<S: SignalingConnection> {
    async fn listener(&self, id: PeerId) -> Result<PeerListener>;
}