use crate::p2p::offer_reply::{OfferReply, OfferReplyId};
use crate::p2p::peer_connection::UnacceptedPeerConnection;
use crate::p2p::peer_connector::PeerConnector;
use crate::ResponseManager;
use anyhow::Result;
use tokio::sync::mpsc;

pub trait SignalingConnection: JsonCommunication {
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, OfferReply>;
    async fn offer(&self, offer: OfferReply) -> Result<OfferReply>;
    async fn reply(&self, reply: OfferReply) -> Result<()>;
}

/* impl SignalingConnection {
    pub async fn send_offer_reply(&self, offer_reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&offer_reply)?;
        self.send_json(value).await
    }
} */

impl<S: SignalingConnection> PeerConnector<S> for S {
    fn get_signaling_connection(&self) -> &S {
        self
    }
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection> {
        self.get_connection_receiver()
    }
}

pub trait JsonCommunication {
    async fn send_json(&self, json: serde_json::Value) -> Result<()>;
}