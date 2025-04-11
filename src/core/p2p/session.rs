use std::collections::HashMap;
use std::sync::Arc;

use crate::core::p2p::offer_reply::{Offer, OfferReply, OfferReplyId};
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::peer_connection::{PeerConnection, UnacceptedPeerConnection};
use crate::core::p2p::peer_connector::{PeerConnectionCreator, PeerListenerCreator};
use crate::core::p2p::signaling_connection::{JsonCommunication, SignalingConnection};
use crate::ResponseManager;
use anyhow::Result;
use futures::{stream::SplitSink, SinkExt, StreamExt};
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

pub(crate) struct PeerListener {
    connection_receiver: mpsc::Receiver<UnacceptedPeerConnection>,
}

impl PeerListener {
    pub(crate) async fn receive(&mut self) -> Option<UnacceptedPeerConnection> {
        self.connection_receiver.recv().await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Session {
    pub(crate) server: String,
    connection_senders: Arc<RwLock<HashMap<PeerId, mpsc::Sender<UnacceptedPeerConnection>>>>,
    response_manager: Arc<ResponseManager<OfferReplyId, OfferReply>>,

    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
}

impl Session {
    pub(crate) async fn handle_packet(&self, packet: Packet) -> Result<()> {
        match packet.r#type.as_ref() {
            "offer" => {
                let connection_senders = self.connection_senders.read().await;
                if let Some(connection_sender) = connection_senders.get(&packet.to) {
                    info!(?packet, "Received offer from signaling server");
                    connection_sender.send(packet).await?;
                } else {
                    warn!("Received offer_reply addressed to {}", &packet.to)
                }
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

    #[tracing::instrument(name = "session_setup")]
    pub(crate) async fn new(server: String) -> Result<Self> {
        info!("Starting session to signaling server at {server}");
        let (ws_stream, _) = tokio_tungstenite::connect_async(server.clone()).await?;
        let (sink, mut stream) = ws_stream.split();

        let response_manager = Arc::new(ResponseManager::new());

        let connection_senders = Arc::new(RwLock::new(HashMap::<
            PeerId,
            mpsc::Sender<UnacceptedPeerConnection>,
        >::new()));

        let session = Self {
            server,
            response_manager,
            connection_senders,
            sink: Arc::new(Mutex::new(sink)),
        };

        tokio::spawn({
            let session = session.clone();
            async move {
                while let Some(message) = stream.next().await {
                    match message {
                        Ok(message) => match message {
                            Message::Text(text) => {
                                let offer_reply: OfferReply =
                                    serde_json::from_str::<OfferReply>(&text).unwrap();
                                info!(?offer_reply, "Received packet from signaling server");
                                session.handle_packet(offer_reply).await.unwrap();
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

        Ok(session)
    }
}

impl PeerListenerCreator<Session> for Session {
    async fn listener(&self, id: String) -> Result<PeerListener> {
        self.register(id.clone()).await?;

        let (connection_sender, connection_receiver) = mpsc::channel(100);

        let mut connection_senders = self.connection_senders.write().await;
        connection_senders.insert(id, connection_sender);

        Ok(PeerListener {
            connection_receiver,
        })
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
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, OfferReply> {
        &self.response_manager
    }
    async fn offer(&self, offer: OfferReply) -> Result<OfferReply> {
        info!(?offer, "Sending offer to signaling server");
        let response_manager = self.get_response_manager();
        let response_receiver = response_manager.wait_for_response(offer.number).await;

        let value = serde_json::to_value(&offer)?;
        self.send_json(value).await?;

        let response = response_receiver.await?;

        Ok(response)
    }
    async fn reply(&self, reply: OfferReply) -> Result<()> {
        info!(?reply, "Sending reply to signaling server");
        let value = serde_json::to_value(&reply)?;
        self.send_json(value).await
        // self.send_offer_reply(reply).await
    }

    async fn register(&self, id: String) -> Result<()> {
        let msg = serde_json::json!({
            "type": "register",
            "id": id,
        });
        let result = self.send_json(msg).await;
        info!(?result, "Registered with signaling server as `{}`", id);
        result
    }
}
