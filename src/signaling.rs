use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfferReply {
    pub r#type: String,
    pub id: String,
    pub to: String,
    pub number: OfferReplyId,
    pub description: String,
}

pub type OfferReplyId = u32;

pub type Offer = OfferReply;
pub type Reply = OfferReply;