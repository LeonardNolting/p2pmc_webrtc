use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use futures::SinkExt;
use rcgen::CertifiedKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_util::sync::CancellationToken;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::certificate::RTCCertificate;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::general::{connect_to_signaling_server, OfferReply, register, SocketTx};
use crate::p2p_helper::{
    create_peer_connection, setup_peer_connection_state_change_listener,
};

pub async fn start_server_proxy(host: &str, id: &str, port: u16, certificate: RTCCertificate) {
    println!("Starting server proxy");
    let id = id.to_owned();
    let host = host.to_owned();

    /*let cancel_token = CancellationToken::new();

    let server_token = cancel_token.clone();*/

    
    // tokio::spawn(async move {
        let signaling_tx = connect_to_signaling_server(
            &host,
            move |offer: OfferReply, signaling_tx| {
                let certificate = certificate.clone();
                async move {
                    tokio::spawn(async move {
                        connect_to_peer_as_listener(offer.clone().description, move |description| {
                            let offer = offer.clone();
                            let signaling_tx = signaling_tx.clone();
                            async move {
                                send_reply_to_offer(offer, &description, signaling_tx).await;
                            }
                        }, port, certificate)
                            .await
                            .unwrap();
                    });
                }
            },
            move |_, _| { async move {} },
            move || {
                async move {}
            }
        ).await;

        register(&id, signaling_tx).await;

        loop {}
    // }).await.unwrap();
    
    // cancel_token;
}

async fn send_reply_to_offer(offer: OfferReply, description: &str, signaling_tx: SocketTx) {
    let reply = OfferReply {
        r#type: "reply".to_string(),
        id: offer.to,
        to: offer.id,
        number: offer.number,
        description: description.to_string(),
    };
    signaling_tx.lock().await.send(Message::Text(Utf8Bytes::from(serde_json::to_string(&reply).unwrap()))).await.expect("Couldn't send reply");
    println!("Sent reply");
}

async fn connect_to_peer_as_listener<F, Fut>(offer: String, push_reply: F, port: u16, certificate: RTCCertificate) -> Result<()>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output=()> + Send + 'static,
{
    let peer_connection = create_peer_connection(certificate).await?;

    let test = peer_connection.sctp().transport().get_remote_certificate().await;

    let minecraft_connection = connect_to_local_server(format!("127.0.0.1:{port}").as_str()).await;
    let (minecraft_read, minecraft_write) = minecraft_connection.into_split();
    let minecraft_write = Arc::new(Mutex::new(minecraft_write));
    let minecraft_read = Arc::new(Mutex::new(minecraft_read));
    println!("Connected to Minecraft server");

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Log changes to connection state
    setup_peer_connection_state_change_listener(&peer_connection, done_tx);

    peer_connection
        .on_data_channel({
            let minecraft_write = minecraft_write.clone();
            let peer_connection = Arc::clone(&peer_connection);
            Box::new(move |data_channel: Arc<RTCDataChannel>| {
                // Ignore all other data channels for now to avoid weird error sources
                if data_channel.label() != "minecraft" { return Box::pin(async {}); }

                println!("Peer created data channel {} {}", data_channel.label(), data_channel.id());

                // Register channel opening handling
                Box::pin({
                    let data_channel = data_channel.clone();
                    let peer_connection = Arc::clone(&peer_connection);
                    let minecraft_read = minecraft_read.clone();
                    let minecraft_write = minecraft_write.clone();
                    async move {
                        data_channel.on_close({
                            let minecraft_write = minecraft_write.clone();
                            Box::new(move || {
                                let minecraft_write = minecraft_write.clone();
                                Box::pin(async move {
                                    print!("Data channel closed, shutting down Minecraft connection...");
                                    minecraft_write.lock().await.shutdown().await.unwrap();
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
                                    minecraft_write.lock().await.shutdown().await.unwrap();
                                    println!("Shut down");
                                })
                            })
                        });

                        data_channel.on_open(Box::new({
                            let data_channel = data_channel.clone();
                            let peer_connection = peer_connection.clone();
                            move || {
                                println!("Data channel '{}'-'{}' open", data_channel.label(), data_channel.id());

                                let minecraft_read = minecraft_read.clone();

                                Box::pin(async move {
                                    let mut buffer = BytesMut::with_capacity(8 * 1024); // 8KB initial buffer

                                    let mut minecraft_read = minecraft_read.lock().await;

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
                                        print!("Sending {bytes_read} bytes from Minecraft server over p2p connection... ");
                                        match data_channel.send(&chunk).await {
                                            Ok(_) => println!("Sent"),
                                            Err(error) => {
                                                println!("Failed to send data from Minecraft server to peer: {error}");
                                                break;
                                            }
                                        }
                                    }
                                    print!("Done handling data_channel.on_open, not sending any more data. Closing connection to peer... ");
                                    peer_connection.close().await.unwrap();
                                    println!("Closed");
                                })
                            }
                        }));

                        // Register text message handling
                        let minecraft_write = minecraft_write.clone();
                        let peer_connection = peer_connection.clone();
                        data_channel.on_message(Box::new(move |msg: DataChannelMessage| {
                            let minecraft_write = minecraft_write.clone();
                            let peer_connection = peer_connection.clone();
                            let len = msg.data.len();
                            print!("Received {len} bytes from peer, forwarding to Minecraft server... ");
                            Box::pin(async move {
                                match minecraft_write.lock().await.write_all(&msg.data).await {
                                    Ok(_) => { println!("Forwarded")}
                                    Err(error) => {
                                        println!("Failed to write to Minecraft server: {error}");
                                        peer_connection.close().await.unwrap();
                                    }
                                }
                            })
                        }));
                    }
                })
            })
        });

    let offer = serde_json::from_str::<RTCSessionDescription>(offer.as_str())?;
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
        push_reply(json_str).await;
    } else {
        println!("generate local_description failed!");
    }

    println!("Waiting for done signal");
    done_rx.recv().await.unwrap();
    println!("Received done signal");

    println!("Closing peer connection and Minecraft connection");
    peer_connection.close().await?;
    minecraft_write.lock().await.shutdown().await?;

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
