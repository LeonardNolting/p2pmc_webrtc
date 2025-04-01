use std::collections::HashMap;
use std::sync::Arc;

use crate::p2p::offer_reply::{Offer, OfferReply, OfferReplyId, Reply};
use crate::p2p::peer::PeerId;
use crate::p2p::peer_connection::{PeerConnection, UnacceptedPeerConnection};
use crate::p2p::signaling_connection::{JsonCommunication, SignalingConnection};
use crate::ResponseManager;
use anyhow::Result;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn, Instrument};

type Packet = OfferReply;

#[derive(Debug)]
pub struct Session {
    pub(crate) server: String,
    instances: Arc<RwLock<HashMap<PeerId, InstanceWriteHalf>>>,
    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
}

#[derive(Debug)]
pub struct Instance {
    pub id: String,
    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    response_manager: Arc<ResponseManager<OfferReplyId, OfferReply>>,
    connection_receiver: mpsc::Receiver<UnacceptedPeerConnection>,
}

#[derive(Debug)]
pub struct InstanceWriteHalf {
    connection_sender: mpsc::Sender<UnacceptedPeerConnection>,
    response_manager: Arc<ResponseManager<OfferReplyId, OfferReply>>,
}

impl InstanceWriteHalf {
    pub async fn handle_packet(&self, packet: Packet) -> Result<()> {
        match packet.r#type.as_ref() {
            "offer" => {
                info!(?packet, "Received offer from signaling server");
                self.connection_sender.send(packet).await?;
            }
            "reply" => {
                info!(?packet, "Received reply from signaling server");
                self.response_manager
                    .handle_response(packet.number, packet)
                    .await;
            }
            r#type => error!(
                r#type,
                "Message was neither offer nor reply, was {}", r#type
            ),
        }

        Ok(())
    }
}

impl Instance {
    async fn send_offer_reply(&self, offer_reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&offer_reply)?;
        self.sink.lock().await.send_json(value).await
    }

    pub async fn accept(&self, offer: Offer) -> Result<PeerConnection> {
        PeerConnection::accept(offer, self).await
    }

    pub async fn connect(&self, to: PeerId) -> Result<PeerConnection> {
        PeerConnection::connect(self.id.clone(), to, self).await
    }
}

impl Session {
    #[tracing::instrument(name = "session_setup")]
    pub async fn new(server: String) -> Result<Self> {
        info!("Starting session to signaling server at {server}");
        let (ws_stream, _) = tokio_tungstenite::connect_async(server.clone()).await?;
        let (sink, mut stream) = ws_stream.split();

        let instances = Arc::new(RwLock::new(HashMap::<PeerId, InstanceWriteHalf>::new()));

        tokio::spawn({
            let instances = Arc::clone(&instances);
            async move {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => match message {
                            Message::Text(text) => {
                                let offer_reply: OfferReply =
                                    serde_json::from_str::<OfferReply>(&text).unwrap();

                                let instances = instances.read().await;
                                if let Some(instance) = instances.get(&offer_reply.to) {
                                    instance.handle_packet(offer_reply).await.unwrap();
                                } else {
                                    warn!("Received offer_reply addressed to {}", &offer_reply.to)
                                }
                            }
                            Message::Close(_) => {
                                info!("Received close frame from signaling server");
                                break;
                            }
                            message => error!(%message, "Unsupported message type sent"),
                        },
                        Err(e) => {
                            error!(%e, "Receiving message failed");
                        }
                    }
                }
                warn!("Disconnected from signaling server");
            }
            .instrument(tracing::info_span!("listener"))
        });

        Ok(Self {
            server,
            instances: instances,
            sink: Arc::new(Mutex::new(sink)),
        })
    }

    pub async fn register(&self, id: String) -> Result<Instance> {
        let msg = serde_json::json!({
            "type": "register",
            "id": id,
        });
        self.sink.lock().await.send_json(msg).await?;
        info!("Registered with signaling server as `{}`", id);

        let (connection_sender, connection_receiver) = mpsc::channel(100);
        let response_manager = Arc::new(ResponseManager::new());
        let instance = Instance {
            id: id.clone(),
            response_manager: response_manager.clone(),
            connection_receiver,
            sink: self.sink.clone(),
        };
        self.instances.write().await.insert(
            id.clone(),
            InstanceWriteHalf {
                connection_sender,
                response_manager,
            },
        );
        Ok(instance)
    }
}

/// Shortcut for sending JSON over WebSocket streams
impl JsonCommunication for SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message> {
    async fn send_json(&mut self, json: Value) -> Result<()> {
        let text = serde_json::to_string(&json)?;
        self.send(Message::Text(Utf8Bytes::from(text))).await?;
        Ok(())
    }
}

impl SignalingConnection for Instance {
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection> {
        &mut self.connection_receiver
    }
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, Reply> {
        &self.response_manager
    }
    async fn offer(&self, offer: Offer) -> Result<Reply> {
        info!(?offer, "Sending offer to signaling server");
        let response_manager = self.get_response_manager();
        let response_receiver = response_manager.wait_for_response(offer.number).await;

        self.send_offer_reply(offer).await?;

        let response = response_receiver.await?;

        Ok(response)
    }
    async fn reply(&self, reply: Reply) -> Result<()> {
        info!(?reply, "Sending reply to signaling server");
        self.send_offer_reply(reply).await
    }
}
