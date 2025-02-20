use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use futures::stream::SplitSink;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::{Error, Message};
use webrtc::data_channel::OnCloseHdlrFn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OfferReply {
    pub(crate) r#type: String,
    pub(crate) id: String,
    pub(crate) to: String,
    pub(crate) number: u32,
    pub(crate) description: String,
}

pub(crate) type SocketTx = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;

// async fn connect_to_signaling_server<Fut: Future, F: (Fn(OfferReply, RawClient) -> Fut) + Send + 'static>(
pub(crate) async fn connect_to_signaling_server<OfferHandler, OfferHandlerFuture, ReplyHandler, ReplyHandlerFuture, DisconnectHandler, DisconnectHandlerFuture>(
    host: &str,
    on_offer: OfferHandler,
    on_reply: ReplyHandler,
    on_disconnect: DisconnectHandler,
) -> SocketTx
where
    OfferHandler: Fn(OfferReply, SocketTx) -> OfferHandlerFuture + Send + Sync + 'static,
    OfferHandlerFuture: Future<Output=()> + Send + 'static,
    ReplyHandler: Fn(OfferReply, SocketTx) -> ReplyHandlerFuture + Send + Sync + 'static,
    ReplyHandlerFuture: Future<Output=()> + Send + 'static,
    DisconnectHandler: Fn() -> DisconnectHandlerFuture + Send + Sync + 'static,
    DisconnectHandlerFuture: Future<Output=()> + Send + 'static,
{
    let host = host.to_owned();
    let on_reply = Arc::new(on_reply);
    let on_offer = Arc::new(on_offer);

    // let (stream, _) = connect_async("ws://127.0.0.1:5100")
    let (stream, _) = connect_async("ws://34.75.203.169:5100")
        .await
        .expect("Couldn't connect to signaling server");

    println!("Connected to signaling server {host}");

    let (outgoing, mut incoming) = stream.split();
    let outgoing: SocketTx = Arc::new(Mutex::new(outgoing));
    tokio::spawn({
        let outgoing = outgoing.clone();
        async move {
            while let Some(message) = incoming.next().await {
                match message {
                    Ok(message) => {
                        match message {
                            Message::Text(text) => {
                                let message: serde_json::Value = serde_json::from_str(&text).unwrap();
                                match message["type"].as_str() {
                                    Some("offer") => {
                                        let message: OfferReply = serde_json::from_value(message).expect("Message didn't have expected format");
                                        println!("Received offer from {}: {}", message.id, message.description);
                                        on_offer(message, outgoing.clone()).await;
                                    },
                                    Some("reply") => {
                                        let message: OfferReply = serde_json::from_value(message).expect("Message didn't have expected format");
                                        println!("Received reply from {}: {}", message.id, message.description);
                                        on_reply(message, outgoing.clone()).await;
                                    },
                                    t => println!("Unsupported message type sent: {:?}", t),
                                }
                            }
                            Message::Close(_) => {
                                println!("Signaling server closed WebSocket connection");
                                break;
                            }
                            message => println!("Unsupported message sent: {message}")
                        }
                    }
                    Err(error) => {
                        println!("Error in WebSocket connection: {error}");
                        break;
                    }
                }
            }

            on_disconnect().await;
        }
    });

    outgoing.clone()
}

pub(crate) async fn register(id: &str, signaling_tx: SocketTx) {
    let mut signaling_tx = signaling_tx.lock().await;
    signaling_tx.send(json!({
        "type": "register",
        "id": id
    }).to_string().into()).await.unwrap();
    println!("Registered on signaling server as {id}");
}