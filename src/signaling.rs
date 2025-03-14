use std::sync::Arc;

use anyhow::Result;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::{Message, error::Error};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio::net::TcpStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OfferReply {
    pub(crate) r#type: String,
    pub(crate) id: String,
    pub(crate) to: String,
    pub(crate) number: u32,
    pub(crate) description: String,
}

pub(crate) type SocketTx = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;

pub(crate) async fn connect_to_signaling_server(
    host: &str,
) -> Result<(SocketTx, UnboundedReceiverStream<OfferReply>), Error> {
    let (ws_stream, _) = connect_async(host).await?;
    println!("Connected to signaling server {}", host);

    let (outgoing, incoming) = ws_stream.split();
    let outgoing = Arc::new(Mutex::new(outgoing));

    let (sender, receiver) = mpsc::unbounded_channel();

    tokio::spawn({
        async move {
            let mut incoming = incoming;
            while let Some(message) = incoming.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<OfferReply>(&text) {
                            Ok(offer_reply) => {
                                if let Err(e) = sender.send(offer_reply) {
                                    eprintln!("Failed to send OfferReply to stream: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to parse OfferReply: {}", e);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Err(e) => {
                        eprintln!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    });

    Ok((outgoing, UnboundedReceiverStream::new(receiver)))
}

pub(crate) async fn register(id: &str, signaling_tx: SocketTx) {
    let mut signaling_tx = signaling_tx.lock().await;
    if let Err(e) = signaling_tx.send(Message::Text(json!({
        "type": "register",
        "id": id
    }).to_string().into())).await {
        eprintln!("Failed to send register message: {}", e);
    }
    println!("Registered on signaling server as {}", id);
}

pub async fn send_offer(offer: OfferReply, signaling_tx: SocketTx) {
    let message = Message::Text(Utf8Bytes::from(
        serde_json::to_string(&offer).unwrap(),
    ));
    if let Err(e) = signaling_tx.lock().await.send(message).await {
        eprintln!("Failed to send offer: {}", e);
    }
    println!("Sent offer");
}

pub async fn send_reply_to_offer(offer: OfferReply, description: &str, signaling_tx: SocketTx) {
    let reply = OfferReply {
        r#type: "reply".to_string(),
        id: offer.to,
        to: offer.id,
        number: offer.number,
        description: description.to_string(),
    };
    let message = Message::Text(Utf8Bytes::from(serde_json::to_string(&reply).unwrap()));
    if let Err(e) = signaling_tx.lock().await.send(message).await {
        eprintln!("Failed to send reply: {}", e);
    }
    println!("Sent reply");
}