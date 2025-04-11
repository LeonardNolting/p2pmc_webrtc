use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OfferReply {
    pub(crate) r#type: String,
    pub(crate) id: String,
    pub(crate) to: String,
    pub(crate) number: OfferReplyId,
    pub(crate) description: String,
}

pub(crate) type OfferReplyId = u32;

pub(crate) type Offer = OfferReply;
pub(crate) type Reply = OfferReply;