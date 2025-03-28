use std::sync::Arc;

use crate::p2p::offer_reply::{OfferReply, OfferReplyId};
use crate::ResponseManager;
use anyhow::Result;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use crate::p2p::peer_connection::UnacceptedPeerConnection;
use crate::p2p::signaling_connection::{JsonCommunication, SignalingConnection};

pub struct Session {
    server: String,
    response_manager: Arc<ResponseManager<OfferReplyId, OfferReply>>,
    connection_receiver: mpsc::Receiver<UnacceptedPeerConnection>,

    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
}

impl Session {
    pub async fn new(server: String) -> Result<Self> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(server.clone()).await?;
        let (sink, mut stream) = ws_stream.split();

        let response_manager = Arc::new(ResponseManager::new());

        let (connection_sender, connection_receiver) = mpsc::channel(100);

        tokio::spawn({
            let response_manager = response_manager.clone();
            async move {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => match message {
                            Message::Text(text) => {
                                let message: serde_json::Value =
                                    serde_json::from_str(&text).unwrap();
                                let r#type = message["type"].as_str().unwrap();
                                match r#type {
                                    "offer" => {
                                        let offer: OfferReply =
                                            serde_json::from_str::<OfferReply>(&text).unwrap();
                                        /* let peer_connection = create_peer_connection(
                                            generate_certificate().await.unwrap(),
                                        )
                                        .await
                                        .unwrap();
                                        let offer_sdp = serde_json::from_str::<RTCSessionDescription>(
                                            &offer.description,
                                        )
                                        .unwrap();
                                        peer_connection
                                            .set_remote_description(offer_sdp)
                                            .await
                                            .unwrap(); */

                                        connection_sender.send(offer).await;
                                    }
                                    "reply" => {
                                        let reply =
                                            serde_json::from_str::<OfferReply>(&text).unwrap();
                                        response_manager.handle_response(reply.number, reply).await;
                                    }
                                    _ => eprintln!("Unsupported message type: {}", r#type),
                                }
                            }
                            Message::Close(_) => {
                                println!("Signaling server closed WebSocket connection");
                                break;
                            }
                            message => println!("Unsupported message sent: {message}"),
                        },
                        Err(e) => {
                            eprintln!("Error receiving message: {}", e);
                        }
                    }
                }
                eprintln!("Disconnected from signaling server");
            }
        });

        Ok(Self {
            server,
            response_manager,
            connection_receiver,
            sink: Arc::new(Mutex::new(sink)),
            // stream: Arc::new(Mutex::new(stream)),
        })
    }
    pub async fn register(&self, id: String) -> Result<()> {
        let msg = serde_json::json!({
            "type": "register",
            "id": id,
        });
        self.send_json(msg).await
    }
}

impl JsonCommunication for Session {
    async fn send_json(&self, json: serde_json::Value) -> Result<()> {
        let text = serde_json::to_string(&json)?;
        let mut sink = self.sink.lock().await;
        sink.send(Message::Text(Utf8Bytes::from(text))).await?;
        Ok(())
    }
}

impl SignalingConnection for Session {
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection> {
        &mut self.connection_receiver
    }
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, OfferReply> {
        &self.response_manager
    }
    async fn offer(&self, offer: OfferReply) -> Result<OfferReply> {
        let response_manager = self.get_response_manager();
        let response_receiver = response_manager.wait_for_response(offer.number).await;

        let value = serde_json::to_value(&offer)?;
        self.send_json(value).await?;

        let response = response_receiver.await?;

        Ok(response)
    }
    async fn reply(&self, reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&reply)?;
        self.send_json(value).await
        // self.send_offer_reply(reply).await
    }
}