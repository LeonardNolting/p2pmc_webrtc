use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::{math_rand_alpha, RTCPeerConnection};

/// Set the handler for Peer connection state
/// This will notify you when the peer has connected/disconnected
pub(crate) fn setup_peer_connection_state_change_listener(peer_connection: &Arc<RTCPeerConnection>, done_tx: Sender<()>) {
    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        println!("Peer Connection State has changed: {s}");

        if s == RTCPeerConnectionState::Failed {
            // Wait until PeerConnection has had no network activity for 30 seconds or another failure. It may be reconnected using an ICE Restart.
            // Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
            // Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
            println!("Peer connection has failed, exiting");
            let _ = done_tx.try_send(());
        }
        
        if s == RTCPeerConnectionState::Closed {
            println!("Peer connection closed, exiting");
            let _ = done_tx.try_send(());
        }

        Box::pin(async {})
    }));
}

pub(crate) async fn create_peer_connection() -> anyhow::Result<Arc<RTCPeerConnection>> {
    // Create a MediaEngine object to configure the supported codec
    let mut m = MediaEngine::default();

    // Register default codecs
    m.register_default_codecs()?;

    // Create a InterceptorRegistry. This is the user configurable RTP/RTCP Pipeline.
    // This provides NACKs, RTCP Reports and other features. If you use `webrtc.NewPeerConnection`
    // this is enabled by default. If you are manually managing You MUST create a InterceptorRegistry
    // for each PeerConnection.
    let mut registry = Registry::new();

    // Use the default set of Interceptors
    registry = register_default_interceptors(registry, &mut m)?;

    // Create the API object with the MediaEngine
    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();

    // Prepare the configuration
    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Create a new RTCPeerConnection
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);
    Ok(peer_connection)
}

pub(crate) fn send_periodic_messages(d2: Arc<RTCDataChannel>) -> Pin<Box<impl Future<Output=()> + Sized>> {
    Box::pin(async move {
        let mut result = anyhow::Result::<usize>::Ok(0);
        while result.is_ok() {
            let timeout = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(timeout);

            tokio::select! {
                    _ = timeout.as_mut() =>{
                        let message = math_rand_alpha(15);
                        println!("Sending '{message}'");
                        result = d2.send_text(message).await.map_err(Into::into);
                    }
                };
        }
    })
}