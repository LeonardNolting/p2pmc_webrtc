use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use rand::random;
use rust_socketio::client::Client;
use rust_socketio::{Payload, RawClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::general::{connect_to_signaling_server, OfferReply, register, test};
use crate::p2p_helper::{
    create_peer_connection, setup_peer_connection_state_change_listener,
};
use crate::reply_manager::ResponseManager;

pub async fn start_client_proxy(host: &str, id: &str) -> Client {
    println!("Starting client proxy");

    let response_manager = ResponseManager::new();

    let response_manager_2 = response_manager.clone();
    let socket = connect_to_signaling_server(
        host,
        move |_, _| { async {} },
        move |reply: OfferReply, socket: RawClient| {
            let response_manager_3 = response_manager_2.clone();
            async move {
                response_manager_3.handle_response(reply.number, reply).await;
            }
        },
    ).await;

    register(id, &socket);

    let id_2 = id.to_owned();
    let socket_2 = socket.clone();

    tokio::spawn(async {
        println!("Starting Minecraft adapter");
        listen_for_minecraft_client_connections("0.0.0.0:25565", move |stream, addr| {
            println!("New connection to Minecraft client adapter");
            let id_3 = id_2.to_owned();
            let socket_3 = socket_2.clone();
            let response_manager_3 = response_manager.clone();
            async move {
                tokio::spawn(async move {
                    connect_to_peer_as_dialer(id_3.parse().unwrap(), "TESTID2".parse().unwrap(), &socket_3, stream, addr, response_manager_3).await.unwrap();
                }).await.unwrap();
            }
        }).await;

        loop {}
    }).await.unwrap();

    socket
}

async fn listen_for_minecraft_client_connections<Fut: Future, F: (Fn(TcpStream, SocketAddr) -> Fut) + Send + 'static>(url: &str, on_connect: F) {
    let listener = TcpListener::bind(url).await.unwrap();
    println!("Listening for TCP connections from Minecraft clients under {url}");
    loop {
        let (stream, address) = listener.accept().await
            .expect(&format!("Couldn't connect to Minecraft client under {url}"));
        stream.set_nodelay(true).unwrap();

        on_connect(stream, address).await;
    }
}

fn send_offer(offer: OfferReply, socket: &Client) {
    // TODO wait for ack? return Future that resolves when ack was received?
    let socket = socket.clone();
    task::spawn_blocking(move || {
        socket
            .emit_with_ack(
                "connections:offer",
                serde_json::to_string(&offer).unwrap(),
                Duration::from_secs(2),
                |message: Payload, _| {
                    println!("connections:offer was acked: {:#?}", message);
                },
            )
            .expect("Server unreachable");
    });
}

async fn connect_to_peer_as_dialer(id: String, to: String, socket: &Client, minecraft_stream: TcpStream, minecraft_client_addr: SocketAddr, reply_manager: Arc<ResponseManager<OfferReply>>) -> Result<()> {
    let peer_connection = create_peer_connection().await?;

    let (mut minecraft_read, minecraft_write) = minecraft_stream.into_split();

    let data_channel = peer_connection
        .create_data_channel(
            "minecraft",
            Some(RTCDataChannelInit {
                ordered: Some(true),
                max_retransmits: None,
                max_packet_life_time: None,
                ..Default::default()
            }),
        )
        .await?;

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Log changes to connection state
    setup_peer_connection_state_change_listener(&peer_connection, done_tx);

    // Register channel opening handling
    let d1 = Arc::clone(&data_channel);
    data_channel.on_open(Box::new(move || {
        println!("Data channel '{}'-'{}' open", d1.label(), d1.id());
        let d2 = Arc::clone(&d1);
        // send_periodic_messages(d2)
        Box::pin(async move {
            let mut buffer = BytesMut::with_capacity(8 * 1024); // 8KB initial buffer

            // task::spawn(async move {
            loop {
                buffer.reserve(1024);
                let bytes_read = minecraft_read.read_buf(&mut buffer).await.unwrap();

                if bytes_read == 0 {
                    // Connection was closed
                    println!("Read 0 bytes from Minecraft client");
                    break;
                }

                // Split off the filled portion and process it
                let chunk = buffer.split().freeze();
                println!("Sending {bytes_read} bytes from Minecraft client over p2p connection");
                d2.send(&chunk).await.unwrap();
                println!("Sent data");
            }
            // }).await.unwrap()
        })
    }));

    // Register text message handling
    let d_label = data_channel.label().to_owned();
    let mut minecraft_write = Some(minecraft_write);
    data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
        let len = msg.data.len();
        println!("Received {len} bytes from peer");
        let data_2 = msg.data.clone();
        let mut minecraft_write = minecraft_write.take().unwrap();
        Box::pin(async move {
            minecraft_write.write_all(&data_2).await.expect("Failed to write to Minecraft server");
            println!("Forwarded data from peer to Minecraft");
        })
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

    let offer_number = random::<u32>();
    let reply_receiver = reply_manager.wait_for_response(offer_number).await;

    // Output the answer in base64 so we can paste it in browser
    if let Some(local_desc) = peer_connection.local_description().await {
        let json_str = serde_json::to_string(&local_desc)?;
        send_offer(OfferReply {
            id,
            to,
            number: offer_number,
            description: json_str.clone(),
        }, socket);
        println!("Pushed offer: {json_str}");
    } else {
        println!("generate local_description failed!");
    }

    let reply = reply_receiver.await.expect("Failed to receive response");
    let desc_data = reply.description;
    let answer = serde_json::from_str::<RTCSessionDescription>(&desc_data)?;

    // Apply the answer as the remote description
    peer_connection.set_remote_description(answer).await?;
    
    println!("Waiting for done signal");
    done_rx.recv().await.unwrap();
    println!("Received done signal");

    println!("Closing peer connection");
    peer_connection.close().await?;
    // TODO close Minecraft connection?

    Ok(())
}