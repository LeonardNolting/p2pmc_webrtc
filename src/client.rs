use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use crate::p2p_helper::{create_peer_connection, must_read_stdin, send_periodic_messages, setup_peer_connection_state_change_listener};
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

// Create RTCPeerConnection
// Create RTCDataChannel on RTCPeerConnection
// Callback on RTCDataChannel when it is opened
//   Send messages to RTCDataChannel
// Callback when data is received on RTCDataChannel
// 

pub(crate) async fn client() -> anyhow::Result<()> {
    let peer_connection = create_peer_connection().await?;

    let data_channel = peer_connection.create_data_channel("minecraft", Some(RTCDataChannelInit {
        ordered: Some(true),
        max_retransmits: None,
        max_packet_life_time: None,
        ..Default::default()
    })).await?;

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    
    // Log changes to connection state
    setup_peer_connection_state_change_listener(&peer_connection, done_tx);

    // Register channel opening handling
    let d1 = Arc::clone(&data_channel);
    data_channel.on_open(Box::new(move || {
        println!("Data channel '{}'-'{}' open. Random messages will now be sent to any connected DataChannels every 5 seconds", d1.label(), d1.id());
        let d2 = Arc::clone(&d1);
        send_periodic_messages(d2)
    }));

    // Register text message handling
    let d_label = data_channel.label().to_owned();
    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let msg_str = String::from_utf8(msg.data.to_vec()).unwrap();
        println!("Message from DataChannel '{d_label}': '{msg_str}'");
        Box::pin(async {})
    }));

    // Create an offer to send to the browser
    let offer = peer_connection.create_offer(None).await?;

    // Create channel that is blocked until ICE Gathering is complete
    let mut gather_complete = peer_connection.gathering_complete_promise().await;

    // Sets the LocalDescription, and starts our UDP listeners
    peer_connection.set_local_description(offer).await?;

    // Block until ICE Gathering is complete, disabling trickle ICE
    // we do this because we only can exchange one signaling message
    // in a production application you should exchange ICE Candidates via OnICECandidate
    let _ = gather_complete.recv().await;

    // Output the answer in base64 so we can paste it in browser
    if let Some(local_desc) = peer_connection.local_description().await {
        let json_str = serde_json::to_string(&local_desc)?;
        // TODO push to signaling server
        println!("{json_str}");
    } else {
        println!("generate local_description failed!");
    }

    // TODO wait for signaling server
    let desc_data = must_read_stdin()?; // .as_str()
    let answer = serde_json::from_str::<RTCSessionDescription>(&desc_data)?;

    // Apply the answer as the remote description
    peer_connection.set_remote_description(answer).await?;

    println!("Press ctrl-c to stop");
    tokio::select! {
        _ = done_rx.recv() => {
            println!("received done signal!");
        }
        _ = tokio::signal::ctrl_c() => {
            println!();
        }
    };

    peer_connection.close().await?;

    Ok(())
}