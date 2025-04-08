use std::future::Future;
use std::sync::Arc;

use crate::core::p2p::peer::PeerId;
use crate::{generate_certificate, ResponseManager};
use anyhow::anyhow;
use anyhow::Result;
use futures::{channel, FutureExt};
use rand::random;
use tokio::sync::oneshot;
use tracing::{error, info};
use webrtc::{
    data::data_channel::DataChannel,
    data_channel::{data_channel_init::RTCDataChannelInit, RTCDataChannel},
    peer_connection::{sdp::session_description::RTCSessionDescription, RTCPeerConnection},
};
use crate::core::p2p::offer_reply::Offer;
use crate::core::p2p::offer_reply::OfferReply;
use crate::core::p2p::signaling_connection::SignalingConnection;
use crate::util::new_rtc_peer_connection::{create_peer_connection, setup_peer_connection_state_change_listener};

#[derive(Clone)]
pub struct PeerConnection {
    pub id: PeerId,
    pub to: PeerId,
    channel_response_manager: Arc<ResponseManager<String, Arc<RTCDataChannel>>>,
    pub peer_connection: Arc<RTCPeerConnection>,
    pub default: Option<Arc<RTCDataChannel>>
}

/// Can be obtained by accepting an offer (listener) or by connecting (dialer)
impl PeerConnection {
    #[tracing::instrument(name = "peer_connect", skip(signaling_connection))]
    pub async fn connect<T: SignalingConnection>(
        id: PeerId,
        to: PeerId,
        signaling_connection: &T,
    ) -> Result<Self> {
        info!("Connecting from `{id}` to `{to}`");
        let rtc_peer_connection = create_peer_connection(generate_certificate().await?).await?;

        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Log changes to connection state
        setup_peer_connection_state_change_listener(&rtc_peer_connection, done_tx);
        
        let mut peer_connection = Self::new(id.clone(), to.clone(), rtc_peer_connection.clone());
        
        // let default_channel_future = peer_connection.open_channel("default".to_string());
        let default_data_channel = peer_connection.create_reliable_data_channel("default").await?;

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
        
        // let _default_channel = default_channel_future.await?;
        Self::wait_for_data_channel_to_open(default_data_channel.clone()).await?;
        peer_connection.default = Some(default_data_channel);

        Ok(peer_connection)
    }

    #[tracing::instrument(name = "peer_accept", skip(signaling_connection))]
    pub async fn accept<T: SignalingConnection>(
        offer: OfferReply,
        signaling_connection: &T,
    ) -> Result<Self> {
        info!(?offer, "Accepting peer connection offer");

        let rtc_peer_connection = create_peer_connection(generate_certificate().await?).await?;

        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Log changes to connection state
        setup_peer_connection_state_change_listener(&rtc_peer_connection, done_tx);
        
        let mut peer_connection = Self::new(
            offer.id.clone(),
            offer.to.clone(),
            rtc_peer_connection.clone(),
        );
        
        let default_channel_future = peer_connection.accept_channel("default".to_string());

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
        
        peer_connection.default = Some(default_channel_future.await.await?);

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
            default: None,
        }
    }

    async fn create_reliable_data_channel(&self, name: &str) -> Result<Arc<RTCDataChannel>> {
        info!(?name, "Creating reliable data channel");
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
        let data_channel = self.open_channel(name).await?;

        Ok(data_channel.detach().await?)
    }

    pub async fn accept_channel(&self, name: String) -> oneshot::Receiver<Arc<RTCDataChannel>> {
        self.channel_response_manager.wait_for_response(name).await
    }
    
    // TODO is this right? why is .map awaited?
    pub async fn accept_channel_detached(&self, name: String) -> impl Future<Output = Arc<DataChannel>> + Send + 'static {
        let data_channel = self.accept_channel(name).await;
        
        data_channel.map(async |data_channel| {
            data_channel.unwrap().detach().await.unwrap()
        }).await
    }
    
    pub async fn close(&self) -> webrtc::error::Result<()> {
        self.peer_connection.close().await
    }
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        let peer_connection = self.peer_connection.clone();
        tokio::spawn(async move {
            let _ = peer_connection.close().await.map_err(|e| {
                error!(%e, "Closing PeerConnection failed");
            });
        });
    }
}

pub type UnacceptedPeerConnection = Offer;