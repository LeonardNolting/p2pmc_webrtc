use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::certificate::RTCCertificate;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::RTCPeerConnection;

pub(crate) async fn create_peer_connection(
    certificate: RTCCertificate,
) -> anyhow::Result<Arc<RTCPeerConnection>> {
    let mut m = MediaEngine::default();
    m.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut m)?;

    // Enable detached data channels
    let mut setting_engine = SettingEngine::default();
    setting_engine.detach_data_channels();

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .with_setting_engine(setting_engine)
        .build();

    let config = RTCConfiguration {
        ice_servers: vec![
            RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                ..Default::default()
            },
            /*RTCIceServer {
                urls: vec!["turn:127.0.0.1:3478".to_owned()],
                username: "dummy".to_owned(), // Not used but required by WebRTC
                credential: "dummy".to_owned(),
                ..Default::default()
            },*/

            RTCIceServer {
                urls: vec!["turn:standard.relay.metered.ca:80".to_owned()],
                username: "48c1aa0fb184480f8cf49f76".to_owned(),
                credential: "oEyJaDELWRetgXeF".to_owned(),
            },
            RTCIceServer {
                urls: vec!["turn:standard.relay.metered.ca:80?transport=tcp".to_owned()],
                username: "48c1aa0fb184480f8cf49f76".to_owned(),
                credential: "oEyJaDELWRetgXeF".to_owned(),
            },
            RTCIceServer {
                urls: vec!["turn:standard.relay.metered.ca:443".to_owned()],
                username: "48c1aa0fb184480f8cf49f76".to_owned(),
                credential: "oEyJaDELWRetgXeF".to_owned(),
            },
            RTCIceServer {
                urls: vec!["turns:standard.relay.metered.ca:443?transport=tcp".to_owned()],
                username: "48c1aa0fb184480f8cf49f76".to_owned(),
                credential: "oEyJaDELWRetgXeF".to_owned(),
            },
        ],
        certificates: vec![certificate],
        ..Default::default()
    };

    Ok(Arc::new(api.new_peer_connection(config).await?))
}

/// Set the handler for Peer connection state
/// This will notify you when the peer has connected/disconnected
#[tracing::instrument(name = "peer_connection_state_change_listener")]
pub(crate) fn setup_peer_connection_state_change_listener(
    peer_connection: &Arc<RTCPeerConnection>,
    done_tx: Sender<()>,
) {
    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        info!("Peer Connection State has changed: {s}");

        if s == RTCPeerConnectionState::Failed {
            // Wait until PeerConnection has had no network activity for 30 seconds or another failure. It may be reconnected using an ICE Restart.
            // Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
            // Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
            error!("Peer connection has failed, exiting");
            let _ = done_tx.try_send(());
        }

        if s == RTCPeerConnectionState::Closed {
            info!("Peer connection closed, exiting");
            let _ = done_tx.try_send(());
        }

        Box::pin(async {})
    }));
}
