use std::collections::HashMap;
use std::ops::Deref;
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
use tokio::sync::mpsc::Receiver;
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{error, info, warn, Instrument};
use crate::p2p::peer_connector::PeerConnector;

type Packet = OfferReply;

#[derive(Debug, Clone)]
pub struct Session {
    pub(crate) server: String,
    instances: Arc<RwLock<HashMap<PeerId, InstanceWriteHalf>>>,
    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
}

/// Provides .receive()
pub trait PeerListener<S: SignalingConnection> {
    fn get_signaling_connection(&self) -> &S;
    async fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    async fn receive(&mut self) -> Option<Offer> {
        self.get_connection_receiver().await.recv().await
    }
}

pub struct InstanceListener {
    pub instance: Instance,
    response_manager: Arc<ResponseManager<OfferReplyId, OfferReply>>,
}

impl PeerListener<Session> for InstanceListener {
    fn get_signaling_connection(&self) -> &Session {
        self.instance
    }

    async fn get_connection_receiver(&mut self) -> &mut Receiver<UnacceptedPeerConnection> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct Instance {
    pub id: String,
    pub(crate) session: Session,
}

impl InstanceListener {
}

impl Drop for Instance {
    fn drop(&mut self) {
        let id = self.id.clone();
        let session = self.session.clone();
        tokio::spawn(async move {
            if let Err(e) = session.deregister(&id).await {
                error!(error = ?e, "Failed to deregister instance");
            }
        });
    }
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
        self.session.sink.lock().await.send_json(value).await
    }
    
    pub async fn accept(&self, offer: Offer) -> Result<PeerConnection> {
        PeerConnection::accept(offer, self).await
    }

    async fn listener(&self) -> Result<InstanceListener> {
        let (connection_sender, connection_receiver) = mpsc::channel(100);
        let response_manager = Arc::new(ResponseManager::new());
        Ok(InstanceListener {
            instance: self,
            
        })
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
            instances,
            sink: Arc::new(Mutex::new(sink)),
        })
    }
    
    pub async fn listener(&self, id: String) -> Result<SessionListener> {
        
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
            session: self.clone(),
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
    
    pub async fn deregister(&self, id: &String) -> Result<()> {
        self.instances.write().await.remove(id);
        Ok(())
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
    async fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection> {
        let test = &mut *self.connection_receiver.lock().await;
        return test
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
