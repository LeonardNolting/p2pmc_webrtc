use crate::p2p::offer_reply::{Offer, OfferReply, OfferReplyId, Reply};
use crate::p2p::peer_connection::UnacceptedPeerConnection;
use crate::p2p::peer_connector::PeerConnectionCreator;
use crate::ResponseManager;
use anyhow::Result;
use tokio::sync::mpsc;

pub trait SignalingConnection: JsonCommunication {
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, Reply>;
    async fn offer(&self, offer: Offer) -> Result<Reply>;
    async fn reply(&self, reply: Reply) -> Result<()>;
}

/* impl SignalingConnection {
    pub async fn send_offer_reply(&self, offer_reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&offer_reply)?;
        self.send_json(value).await
    }
} */

impl<S: SignalingConnection> PeerConnectionCreator<S> for S {
    fn get_signaling_connection(&self) -> &S {
        self
    }
}

pub trait JsonCommunication {
    async fn send_json(&self, json: serde_json::Value) -> Result<()>;
}