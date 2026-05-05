use anyhow::Result;

use crate::core::p2p::offer_reply::{Offer, OfferReplyId, Reply};
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::peer_connector::PeerConnectionCreator;
use crate::util::response_manager::ResponseManager;

pub(crate) trait SignalingConnection: JsonCommunication {
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, Reply>;
    async fn offer(&self, offer: Offer) -> Result<Reply>;
    async fn reply(&self, reply: Reply) -> Result<()>;
    async fn register(&self, id: PeerId) -> Result<()>;
}

/* impl SignalingConnection {
    pub(crate) async fn send_offer_reply(&self, offer_reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&offer_reply)?;
        self.send_json(value).await
    }
} */

impl<S: SignalingConnection> PeerConnectionCreator<S> for S {
    fn get_signaling_connection(&self) -> &S {
        self
    }
}

pub(crate) trait JsonCommunication {
    async fn send_json(&self, json: serde_json::Value) -> Result<()>;
}