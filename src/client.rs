use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use futures::SinkExt;
use rand::random;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use url::Url;
use crate::general::{connect_to_signaling_server, OfferReply, register, SocketTx};
use crate::parse_server::{get_server_address, parse_server};
use crate::log_on_drop::LogOnDrop;
use crate::p2p_helper::{
    create_peer_connection, setup_peer_connection_state_change_listener,
};
use crate::reply_manager::ResponseManager;

pub async fn start_client_proxy(host: &str, id: &str) {
    println!("Starting client proxy");

    let response_manager = ResponseManager::new();

    let signaling_tx = connect_to_signaling_server(
        host,
        move |_, _| { async {} },
        {
            let response_manager = response_manager.clone();
            move |reply: OfferReply, _| {
                let response_manager = response_manager.clone();
                async move {
                    response_manager.handle_response(reply.number, reply).await;
                }
            }
        },
        move || {
            async move {}
        },
    ).await;

    register(id, signaling_tx.clone()).await;

    let id = id.to_owned();
    let signaling_tx = signaling_tx.clone();

    println!("Starting Minecraft adapter");
    listen_for_minecraft_client_connections("0.0.0.0:25565", {
        let id = id.clone();
        let signaling_tx = signaling_tx.clone();
        let response_manager = response_manager.clone();
        move |mut stream, addr| {
            println!("New connection to Minecraft client adapter");
            let id = id.clone();
            let signaling_tx = signaling_tx.clone();
            let response_manager = response_manager.clone();
            async move {
                tokio::spawn(async move {
                    // TODO any of the parsing fails, close all connections - need to retry
                    let to_id = parse_server(&mut stream).await.unwrap();
                    connect_to_peer_as_dialer(id.parse().unwrap(), to_id, signaling_tx, stream, addr, response_manager).await.unwrap();
                }).await.unwrap();
            }
        }
    }).await;

    print!("Minecraft adapter closed. Closing signaling connection... ");
    signaling_tx.clone().lock().await.close().await.unwrap();
    println!("Closed");
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

async fn send_offer(offer: OfferReply, signaling_tx: SocketTx) {
    signaling_tx.lock().await.send(Message::Text(Utf8Bytes::from(serde_json::to_string(&offer).unwrap()))).await.expect("Couldn't send offer");
    println!("Sent offer");
}

async fn connect_to_peer_as_dialer(id: String, to: String, socket: SocketTx, minecraft_stream: TcpStream, _minecraft_client_addr: SocketAddr, reply_manager: Arc<ResponseManager<OfferReply>>) -> Result<()> {
    let peer_connection = create_peer_connection().await?;

    let (minecraft_read, minecraft_write) = minecraft_stream.into_split();
    let mut minecraft_read = LogOnDrop::new(minecraft_read, "minecraft_read");
    let minecraft_write = Arc::new(Mutex::new(LogOnDrop::new(minecraft_write, "minecraft_write")));

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

    // Forward messages from Minecraft client to peer
    data_channel.on_open({
        let data_channel = Arc::clone(&data_channel);
        let peer_connection = Arc::clone(&peer_connection);
        Box::new(move || {
            println!("Data channel '{}'-'{}' open", data_channel.label(), data_channel.id());
            Box::pin(async move {
                let mut buffer = BytesMut::with_capacity(8 * 1024); // 8KB initial buffer

                loop {
                    buffer.reserve(1024);
                    let bytes_read = minecraft_read.inner.read_buf(&mut buffer).await.unwrap();

                    if bytes_read == 0 {
                        // Connection was closed
                        println!("Read 0 bytes from Minecraft client");
                        break;
                    }

                    // Split off the filled portion and process it
                    let chunk = buffer.split().freeze();
                    print!("Sending {bytes_read} bytes from Minecraft client over p2p connection... ");
                    match data_channel.send(&chunk).await {
                        Ok(_) => println!("Sent"),
                        Err(error) => {
                            println!("Failed to send data from Minecraft client to peer: {error}");
                            break;
                        }
                    }
                }
                print!("Done handling data_channel.on_open, not sending any more data. Closing connection to peer... ");
                peer_connection.close().await.unwrap();
                println!("Closed");
            })
        })
    });

    // Register text message handling
    data_channel.on_message({
        let minecraft_write = minecraft_write.clone();
        let peer_connection = peer_connection.clone();
        Box::new(move |msg: DataChannelMessage| {
            let minecraft_write = minecraft_write.clone();
            let peer_connection = peer_connection.clone();
            let len = msg.data.len();
            print!("Received {len} bytes from peer, forwarding to Minecraft client... ");
            Box::pin(async move {
                match minecraft_write.lock().await.inner.write_all(&msg.data).await {
                    Ok(_) => { println!("Forwarded") }
                    Err(error) => {
                        println!("Failed to write to Minecraft client: {error}");
                        peer_connection.close().await.unwrap();
                    }
                }
            })
        })
    });

    data_channel.on_close({
        let minecraft_write = minecraft_write.clone();
        Box::new(move || {
            let minecraft_write = minecraft_write.clone();
            Box::pin(async move {
                print!("Data channel closed, shutting down Minecraft connection...");
                minecraft_write.lock().await.inner.shutdown().await.unwrap();
                println!("Shut down");
            })
        })
    });

    data_channel.on_error({
        let minecraft_write = minecraft_write.clone();
        Box::new(move |error| {
            let minecraft_write = minecraft_write.clone();
            Box::pin(async move {
                println!("Data channel got an error: {error}");
                print!("Shutting down Minecraft connection...");
                minecraft_write.lock().await.inner.shutdown().await.unwrap();
                println!("Shut down");
            })
        })
    });

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

    if let Some(local_desc) = peer_connection.local_description().await {
        let json_str = serde_json::to_string(&local_desc)?;
        send_offer(OfferReply {
            r#type: "offer".to_string(),
            id,
            to,
            number: offer_number,
            description: json_str.clone(),
        }, socket).await;
        println!("Pushed offer: {json_str}");
    } else {
        println!("Generating local_description failed!");
    }

    let reply = reply_receiver.await.expect("Failed to receive response");
    let answer = serde_json::from_str::<RTCSessionDescription>(&reply.description)?;
    peer_connection.set_remote_description(answer).await?;

    println!("Waiting for done signal");
    done_rx.recv().await.unwrap();
    println!("Received done signal");

    println!("Closing peer connection and Minecraft connection");
    peer_connection.close().await?;
    minecraft_write.lock().await.inner.shutdown().await.unwrap();

    Ok(())
}