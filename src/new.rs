use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use futures::{channel, stream::SplitSink, SinkExt, StreamExt};
use rand::random;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
};
use tokio_tungstenite::{
    tungstenite::{Message, Utf8Bytes},
    MaybeTlsStream, WebSocketStream,
};
use webrtc::{
    data::data_channel::DataChannel,
    data_channel::{data_channel_init::RTCDataChannelInit, RTCDataChannel},
    peer_connection::{sdp::session_description::RTCSessionDescription, RTCPeerConnection},
};

use crate::signaling::{Offer, OfferReplyId};
use crate::{create_peer_connection, generate_certificate, signaling::OfferReply, ResponseManager};

pub struct Peer {
    pub id: PeerId,
}

pub type PeerId = String;

// TODO store signaling connections etc? store open connections to other peers?
impl Peer {}

pub struct PeerConnection {
    pub id: PeerId,
    pub to: PeerId,
    channel_response_manager: Arc<ResponseManager<String, Arc<RTCDataChannel>>>,
    pub peer_connection: Arc<RTCPeerConnection>,
}

/// Can be obtained by accepting an offer (listener) or by connecting (dialer)
impl PeerConnection {
    pub async fn connect<T: SignalingConnection>(
        id: PeerId,
        to: PeerId,
        signaling_connection: &T,
    ) -> Result<Self> {
        let rtc_peer_connection = create_peer_connection(generate_certificate().await?).await?;
        let peer_connection = Self::new(id.clone(), to.clone(), rtc_peer_connection.clone());

        let offer = rtc_peer_connection.create_offer(None).await?;

        // Create channel that is blocked until ICE Gathering is complete
        let mut gather_complete = rtc_peer_connection.gathering_complete_promise().await;

        // Sets the LocalDescription, and starts our UDP listeners
        rtc_peer_connection.set_local_description(offer).await?;

        // Block until ICE Gathering is complete, disabling trickle ICE
        // we do this because we only can exchange one signaling message
        // in a production application you should exchange ICE Candidates via OnICECandidate
        let _ = gather_complete.recv().await;

        let local_description = rtc_peer_connection
            .local_description()
            .await
            .ok_or_else(|| anyhow!("No local description"))?;
        let json_str = serde_json::to_string(&local_description)?;

        let offer_number = random::<u32>();
        let reply = signaling_connection
            .offer(OfferReply {
                r#type: "offer".to_string(),
                id,
                to,
                number: offer_number,
                description: json_str.clone(),
            })
            .await?;
        let answer = serde_json::from_str::<RTCSessionDescription>(&reply.description)?;
        rtc_peer_connection.set_remote_description(answer).await?;

        Ok(peer_connection)
    }

    pub async fn accept<T: SignalingConnection>(
        offer: OfferReply,
        signaling_connection: &T,
    ) -> Result<Self> {
        let rtc_peer_connection = create_peer_connection(generate_certificate().await?).await?;
        let peer_connection = Self::new(
            offer.id.clone(),
            offer.to.clone(),
            rtc_peer_connection.clone(),
        );

        // TODO just use RTCSessionDescription in OfferReply for automatic serialization and deserialization
        let description =
            serde_json::from_str::<RTCSessionDescription>(offer.description.as_str())?;
        rtc_peer_connection
            .set_remote_description(description)
            .await?;

        // Create an answer
        let answer = rtc_peer_connection.create_answer(None).await?;

        // Create channel that is blocked until ICE Gathering is complete
        let mut gather_complete = rtc_peer_connection.gathering_complete_promise().await;

        // Sets the LocalDescription, and starts our UDP listeners
        rtc_peer_connection.set_local_description(answer).await?;

        // Block until ICE Gathering is complete, disabling trickle ICE
        // we do this because we only can exchange one signaling message
        // in a production application you should exchange ICE Candidates via OnICECandidate
        let _ = gather_complete.recv().await;

        let local_description = rtc_peer_connection
            .local_description()
            .await
            .ok_or_else(|| anyhow!("No local description"))?;
        signaling_connection
            .reply(OfferReply {
                r#type: "reply".to_string(),
                id: offer.to,
                to: offer.id,
                number: offer.number,
                description: serde_json::to_string(&local_description)?,
            })
            .await?;

        Ok(peer_connection)
    }

    fn new(id: PeerId, to: PeerId, peer_connection: Arc<RTCPeerConnection>) -> Self {
        let channel_response_manager = Arc::new(ResponseManager::new());
        peer_connection.on_data_channel({
            let channel_response_manager = channel_response_manager.clone();
            Box::new(move |data_channel: Arc<RTCDataChannel>| {
                let channel_response_manager = channel_response_manager.clone();
                Box::pin(async move {
                    channel_response_manager
                        .handle_response(data_channel.label().to_string(), data_channel).await;
                })
            })
        });

        // TODO create default data channel for signaling
        // and implement PeerConnector for PeerConnection

        Self {
            id,
            to,
            peer_connection,
            channel_response_manager,
        }
    }

    async fn create_reliable_data_channel(&self, name: &str) -> Result<Arc<RTCDataChannel>> {
        Ok(self
            .peer_connection
            .create_data_channel(
                name,
                Some(RTCDataChannelInit {
                    ordered: Some(true),
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await?)
    }
    pub async fn wait_for_data_channel_to_open(data_channel: Arc<RTCDataChannel>) -> Result<()> {
        let (on_open_tx, on_open_rx) = channel::oneshot::channel();
        data_channel.on_open(Box::new(move || {
            let _ = on_open_tx.send(());
            Box::pin(async {})
        }));
        on_open_rx.await?;
        Ok(())
    }
    pub async fn open_channel(&self, name: String) -> Result<Arc<RTCDataChannel>> {
        let data_channel = self.create_reliable_data_channel(&name).await?;

        Self::wait_for_data_channel_to_open(data_channel.clone()).await?;

        /* let (on_open_tx, on_open_rx) = tokio::sync::oneshot::channel();
        let on_open_tx = Arc::new(Mutex::new(Some(on_open_tx)));

        data_channel.clone().on_open(Box::new(move || {
            let detached_sender = on_open_tx.clone();
            Box::pin(async move {
                let mut guard = detached_sender.lock().await;
                if let Some(sender) = guard.take() {
                    let _ = sender.send(());
                }
            })
        }));

        on_open_rx.await?; */

        Ok(data_channel.clone())
    }

    pub async fn open_detached_channel(&self, name: String) -> Result<Arc<DataChannel>> {
        let data_channel = self.open_channel(name).await.unwrap();

        Ok(data_channel.detach().await?)
    }

    pub async fn accept_channel(&self, name: String) -> oneshot::Receiver<Arc<RTCDataChannel>> {
        self.channel_response_manager.wait_for_response(name).await
    }
}

pub trait SignalingConnection: JsonCommunication {
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    fn get_response_manager(&self) -> &ResponseManager<OfferReplyId, OfferReply>;
    async fn offer(&self, offer: OfferReply) -> Result<OfferReply>;
    async fn reply(&self, reply: OfferReply) -> Result<()>;
}

/* impl SignalingConnection {
    pub async fn send_offer_reply(&self, offer_reply: OfferReply) -> Result<()> {
        let value = serde_json::to_value(&offer_reply)?;
        self.send_json(value).await
    }
} */

type UnacceptedPeerConnection = Offer;

pub trait PeerConnector<T: SignalingConnection> {
    fn get_self(&self) -> &T;
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection>;
    async fn connect(&self, id: PeerId, to: PeerId) -> Result<PeerConnection> {
        PeerConnection::connect::<T>(id, to, self.get_self()).await
    }
    async fn accept(&mut self) -> Option<Offer> {
        self.get_connection_receiver().recv().await
    }
}

impl<T: SignalingConnection> PeerConnector<T> for T {
    fn get_self(&self) -> &T {
        self
    }
    fn get_connection_receiver(&mut self) -> &mut mpsc::Receiver<UnacceptedPeerConnection> {
        self.get_connection_receiver()
    }
}

pub trait JsonCommunication {
    async fn send_json(&self, json: serde_json::Value) -> Result<()>;
}

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
