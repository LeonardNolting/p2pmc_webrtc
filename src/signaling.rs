use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, Message},
    MaybeTlsStream, WebSocketStream,
};
use webrtc::data_channel::data_channel_message::DataChannelMessage;

use crate::new::OfferReplyId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfferReply {
    pub r#type: String,
    pub id: String,
    pub to: String,
    pub number: OfferReplyId,
    pub description: String,
}

pub type Offer = OfferReply;
pub type Reply = OfferReply;