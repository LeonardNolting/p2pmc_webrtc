use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use futures::SinkExt;
use rcgen::CertifiedKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_util::sync::CancellationToken;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::certificate::RTCCertificate;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

pub async fn connect_to_local_server(url: &str) -> TcpStream {
    // set up a connection to the Minecraft server
    println!("Connecting to Minecraft server at {url}");
    let stream = TcpStream::connect(url)
        .await
        .expect(&format!("Couldn't connect to Minecraft server under {url}"));
    stream.set_nodelay(true).unwrap();
    stream
}

pub async fn listen_for_minecraft_client_connections<
    Fut: Future,
    F: (Fn(TcpStream, SocketAddr) -> Fut) + Send + 'static,
>(
    url: &str,
    on_connect: F,
) {
    println!("Starting Minecraft client adapter under {url}");
    let listener = TcpListener::bind(url).await.unwrap();
    println!("Listening for TCP connections from Minecraft clients under {url}");
    loop {
        let (stream, address) = listener
            .accept()
            .await
            .expect(&format!("Couldn't connect to Minecraft client under {url}"));
        stream.set_nodelay(true).unwrap();

        on_connect(stream, address).await;
    }
}