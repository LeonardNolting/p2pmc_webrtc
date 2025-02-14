use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use rust_socketio::{Payload, RawClient};
use rust_socketio::client::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::general::{connect_to_signaling_server, OfferReply, register};
use crate::p2p_helper::{
    create_peer_connection, setup_peer_connection_state_change_listener,
};

pub async fn start_server_proxy(host: &str, id: &str) {
    println!("Starting server proxy");

    let socket = connect_to_signaling_server(
    // connect_to_signaling_server(
        host,
        move |offer: OfferReply, socket| {
            async move {
                println!("ON OFFER ARRIVED");
                let offer = &offer;
                connect_to_peer_as_listener(offer.description.clone(), move |description| {
                    send_reply_to_offer(offer.clone(), &description, &socket);
                })
                .await
                .unwrap();
            }
        },
        move |_, _| {async move {}},
    ).await;

    println!("Connected to server proxy");

    register(id, &socket);

    loop {

    }

    // socket
}

fn send_reply_to_offer(offer: OfferReply, description: &String, socket: &RawClient) {
    let reply = OfferReply {
        id: offer.to,
        to: offer.id,
        number: offer.number,
        description: description.clone(),
    };
    // TODO wait for ack? return Future that resolves when ack was received?
    let socket = socket.clone();
    task::spawn_blocking(move || {
        socket
            .emit_with_ack(
                "connections:reply",
                serde_json::to_string(&reply).unwrap(),
                Duration::from_secs(2),
                |message: Payload, _| {
                    println!("connections:reply was acked: {:#?}", message);
                },
            )
            .expect("Server unreachable");
    });
}

async fn connect_to_peer_as_listener(offer: String, push_reply: impl Fn(String)) -> Result<()> {
    let peer_connection = create_peer_connection().await?;

    let minecraft_connection = connect_to_local_server("127.0.0.1:3000").await;
    let (minecraft_read, minecraft_write) = minecraft_connection.into_split();
    let mut minecraft_read = Some(minecraft_read);
    let mut minecraft_write = Some(minecraft_write);
    println!("Connected to Minecraft server");

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Log changes to connection state
    setup_peer_connection_state_change_listener(&peer_connection, done_tx);

    // Register data channel creation handling
    peer_connection
        .on_data_channel(Box::new(move |d: Arc<RTCDataChannel>| {
            let d_label = d.label().to_owned();

            // Ignore all other data channels for now to avoid weird error sources
            if d_label != "minecraft" { return Box::pin(async {}); }

            let d_id = d.id();
            println!("Peer opened DataChannel {d_label} {d_id}");

            let mut minecraft_read = minecraft_read.take();
            let mut minecraft_write = minecraft_write.take();

            // Register channel opening handling
            Box::pin(async move {
                let d2 = Arc::clone(&d);
                let d_label2 = d_label.clone();
                let d_id2 = d_id;

                d.on_close(Box::new(move || {
                    println!("Data channel closed");
                    Box::pin(async {})
                }));

                let mut minecraft_read = minecraft_read.take().unwrap();

                d.on_open(Box::new({
                    let d = d.clone();
                    move || {
                        println!("Data channel '{d_label2}'-'{d_id2}' open");

                        // send_periodic_messages(d2)

                        Box::pin(async move {
                            let mut buffer = BytesMut::with_capacity(8 * 1024); // 8KB initial buffer

                            // task::spawn(async move {
                                loop {
                                    buffer.reserve(1024);
                                    let bytes_read = minecraft_read.read_buf(&mut buffer).await.unwrap();

                                    if bytes_read == 0 {
                                        // Connection was closed
                                        println!("Read 0 bytes from Minecraft server");
                                        break;
                                    }

                                    // Split off the filled portion and process it
                                    let chunk = buffer.split().freeze();
                                    println!("Sending {bytes_read} bytes from Minecraft server over p2p connection");
                                    d.send(&chunk).await.unwrap();
                                    println!("Sent data");
                                }
                            // }).await.unwrap()
                        })
                    }
                }));

                // Register text message handling
                let mut minecraft_write = minecraft_write.take();
                d.on_message(Box::new(move |msg: DataChannelMessage| {
                    let len = msg.data.len();
                    println!("Received {len} bytes from peer");
                    let mut minecraft_write = minecraft_write.take().unwrap();
                    Box::pin(async move {
                        minecraft_write.write_all(&msg.data).await.expect("Failed to write to Minecraft server");
                        println!("Forwarded data from peer to Minecraft");
                    })
                }));
            })
        }));

    let desc_data = offer.as_str();
    let offer = serde_json::from_str::<RTCSessionDescription>(desc_data)?;

    // Set the remote SessionDescription
    peer_connection.set_remote_description(offer).await?;

    // Create an answer
    let answer = peer_connection.create_answer(None).await?;

    // Create channel that is blocked until ICE Gathering is complete
    let mut gather_complete = peer_connection.gathering_complete_promise().await;

    // Sets the LocalDescription, and starts our UDP listeners
    peer_connection.set_local_description(answer).await?;

    // Block until ICE Gathering is complete, disabling trickle ICE
    // we do this because we only can exchange one signaling message
    // in a production application you should exchange ICE Candidates via OnICECandidate
    let _ = gather_complete.recv().await;

    if let Some(local_desc) = peer_connection.local_description().await {
        let json_str = serde_json::to_string(&local_desc)?;
        push_reply(json_str);
    } else {
        println!("generate local_description failed!");
    }

    println!("Waiting for done signal");
    done_rx.recv().await.unwrap();
    println!("Received done signal");

    println!("Closing peer connection");
    peer_connection.close().await?;
    // TODO close Minecraft connection?

    Ok(())
}

async fn connect_to_local_server(url: &str) -> TcpStream {
    // set up a connection to the Minecraft server
    println!("Connecting to Minecraft server at {url}");
    let stream = TcpStream::connect(url)
        .await
        .expect(&format!("Couldn't connect to Minecraft server under {url}"));
    stream.set_nodelay(true).unwrap();
    stream
}
