use crate::p2p::offer_reply::{Offer, OfferReply, OfferReplyId, Reply};
use crate::p2p::peer_connection::UnacceptedPeerConnection;
use crate::p2p::peer_connector::PeerConnector;
use crate::ResponseManager;
use anyhow::Result;
use tokio::sync::mpsc;

pub trait SignalingConnection {
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, Reply>;
    async fn offer(&self, offer: Offer) -> Result<Reply>;
    async fn reply(&self, reply: Reply) -> Result<()>;
}

impl<S: SignalingConnection> PeerConnector<S> for S {
    fn get_signaling_connection(&self) -> &S {
        self
    }
}

pub trait JsonCommunication {
    async fn send_json(&mut self, json: serde_json::Value) -> Result<()>;
}