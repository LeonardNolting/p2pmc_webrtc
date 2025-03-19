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

#[derive(Debug)]
pub enum SignalingEvent {
    Offer(OfferReply),
    Reply(OfferReply),
    Unknown(Value),
}

pub struct SignalingSender {
    sink: Arc<Mutex<tokio_tungstenite::tungstenite::MessageSink<MaybeTlsStream<tokio::net::TcpStream>>>>,
}

pub struct SignalingReceiver {
    stream: tokio_tungstenite::tungstenite::MessageStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

pub struct SignalingConnection {
    sender: SignalingSender,
    receiver: SignalingReceiver,
}

impl SignalingConnection {
    pub async fn connect(host: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(host).await?;
        let (sink, stream) = ws_stream.split();
        Ok(Self {
            sender: SignalingSender {
                sink: Arc::new(Mutex::new(sink)),
            },
            receiver: SignalingReceiver { stream },
        })
    }

    pub fn split(self) -> (SignalingSender, SignalingReceiver) {
        (self.sender, self.receiver)
    }
}

impl SignalingSender {
    pub async fn register(&self, id: &str) -> Result<()> {
        let msg = json!({
            "type": "register",
            "id": id,
        });
        self.send_json(msg).await
    }

    pub async fn send_offer_reply(&self, msg: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&msg)?;
        self.send_json(value).await
    }

    async fn send_json(&self, msg: Value) -> Result<()> {
        let text = serde_json::to_string(&msg)?;
        let mut sink = self.sink.lock().await;
        sink.send(Message::Text(text)).await?;
        Ok(())
    }
}

impl Clone for SignalingSender {
    fn clone(&self) -> Self {
        Self {
            sink: self.sink.clone(),
        }
    }
}

impl SignalingReceiver {
    pub async fn next_event(&mut self) -> Option<Result<SignalingEvent, Error>> {
        loop {
            let msg = self.stream.next().await?;
            match msg {
                Ok(Message::Text(text)) => {
                    let value: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e.into())),
                    };
                    match value.get("type").and_then(|t| t.as_str()) {
                        Some("offer") => {
                            match serde_json::from_value(value) {
                                Ok(offer) => return Some(Ok(SignalingEvent::Offer(offer))),
                                Err(e) => return Some(Err(e.into())),
                            }
                        }
                        Some("reply") => {
                            match serde_json::from_value(value) {
                                Ok(reply) => return Some(Ok(SignalingEvent::Reply(reply))),
                                Err(e) => return Some(Err(e.into())),
                            }
                        }
                        _ => return Some(Ok(SignalingEvent::Unknown(value))),
                    }
                }
                Ok(Message::Close(_)) => return None,
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl Stream for SignalingReceiver {
    type Item = Result<SignalingEvent, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx).map(|opt| {
            opt.map(|res| {
                res.and_then(|msg| {
                    if let Message::Text(text) = msg {
                        let value: Value = serde_json::from_str(&text)?;
                        Ok(match value.get("type").and_then(|t| t.as_str()) {
                            Some("offer") => {
                                SignalingEvent::Offer(serde_json::from_value(value)?)
                            }
                            Some("reply") => {
                                SignalingEvent::Reply(serde_json::from_value(value)?)
                            }
                            _ => SignalingEvent::Unknown(value),
                        })
                    } else {
                        Err(Error::Protocol("Expected text message".into()))
                    }
                })
            })
        })
    }
}