use std::sync::Arc;

use anyhow::Result;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::{Message, error::Error};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio::net::TcpStream;
use futures::{Sink, Stream};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OfferReply {
    pub(crate) r#type: String,
    pub(crate) id: String,
    pub(crate) to: String,
    pub(crate) number: u32,
    pub(crate) description: String,
}

pub struct SignalingConnection {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl SignalingConnection {
    pub async fn connect(host: &str) -> Result<Self, Error> {
        let (ws_stream, _) = connect_async(host).await?;
        Ok(Self { inner: ws_stream })
    }

    pub async fn send(&mut self, msg: &impl Serialize) -> Result<()> {
        let message = Message::Text(serde_json::to_string(msg).unwrap().into());
        SinkExt::send(&mut self.inner, message).await?;
        Ok(())
    }

    pub async fn register(&mut self, id: &str) -> Result<()> {
        self.send(&json!({ "type": "register", "id": id })).await
    }

    pub fn into_split(self) -> (SignalingSink, SignalingStream) {
        let (sink, stream) = self.inner.split();
        (
            SignalingSink { inner: sink },
            SignalingStream { inner: stream }
        )
    }
}

// Implement direct Stream/Sink for unified usage
impl Stream for SignalingConnection {
    type Item = Result<OfferReply, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx).map(|opt| {
            opt.map(|res| {
                res.and_then(|msg| match msg {
                    Message::Text(text) => serde_json::from_str(&text)
                        .map_err(|e| Error::Protocol(e.into())),
                    _ => Err(Error::Protocol("Unexpected non-text message".into()))
                })
            })
        })
    }
}

impl Sink<OfferReply> for SignalingConnection {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: OfferReply) -> Result<(), Self::Error> {
        let msg = Message::Text(serde_json::to_string(&item).unwrap().into());
        Pin::new(&mut self.inner).start_send(msg)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

// Split types for when you need split ownership
pub struct SignalingSink {
    inner: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

pub struct SignalingStream {
    inner: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl Stream for SignalingStream {
    type Item = Result<OfferReply, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Similar poll logic as above
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl Sink<OfferReply> for SignalingSink {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: OfferReply) -> Result<(), Self::Error> {
        let msg = Message::Text(serde_json::to_string(&item).unwrap().into());
        Pin::new(&mut self.inner).start_send(msg)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
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