use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use reply_manager::ResponseManager;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tokio_stream::StreamExt;
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors,
        media_engine::MediaEngine,
        setting_engine::SettingEngine,
        APIBuilder,
    }, data_channel::{data_channel_init::RTCDataChannelInit, RTCDataChannel}, ice_transport::ice_server::RTCIceServer, interceptor::registry::Registry, peer_connection::{
        certificate::RTCCertificate, configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription, RTCPeerConnection
    }
};

mod signaling;
mod tcp_helpers;
mod reply_manager;
use signaling::{connect_to_signaling_server, register, send_offer, send_reply_to_offer, OfferReply, SocketTx};
use tcp_helpers::{connect_to_local_server, listen_for_minecraft_client_connections};

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[clap(long, default_value = "testserver")]
        id: String,
        // #[clap(long, default_value = "localhost:3000")]
        #[clap(long, default_value = "serveo.net:3001")]
        minecraft_server: String,
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
    },
    Client {
        #[clap(long, default_value = "client")]
        id: String,
        #[clap(long, default_value = "ws://34.75.203.169:5100")]
        signaling_server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Server {
            id,
            minecraft_server,
            signaling_server,
        } => run_server_proxy(&signaling_server, &id, &minecraft_server).await,
        Command::Client {
            id,
            signaling_server,
        } => run_client_proxy(&signaling_server, &id).await,
    }
}

async fn run_server_proxy(signaling_host: &str, id: &str, minecraft_server: &str) -> Result<()> {
    let certificate = generate_certificate().await?;

    let (signaling_tx, mut offer_reply_stream) = connect_to_signaling_server(signaling_host)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to signaling server: {}", e))?;

    register(id, signaling_tx.clone()).await;

    let certificate_clone = certificate.clone();
    let minecraft_server_clone = minecraft_server.to_string();
    let signaling_tx_clone = signaling_tx.clone();

    tokio::spawn(async move {
        while let Some(offer_reply) = offer_reply_stream.next().await {
            if offer_reply.r#type == "offer" {
                if let Err(e) = handle_server_offer(
                    offer_reply,
                    signaling_tx_clone.clone(),
                    &certificate_clone,
                    &minecraft_server_clone,
                )
                .await
                {
                    eprintln!("Error handling server offer: {}", e);
                }
            } else {
                eprintln!("Unexpected message type: {}", offer_reply.r#type);
            }
        }
        eprintln!("Disconnected from signaling server");
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn handle_server_offer(
    offer: OfferReply,
    signaling_tx: SocketTx,
    certificate: &RTCCertificate,
    minecraft_server: &str,
) -> Result<()> {
    let peer_connection = create_peer_connection(certificate.clone()).await?;

    let (detached_sender, detached_receiver) = tokio::sync::oneshot::channel();
    let sender_container = Arc::new(Mutex::new(Some(detached_sender)));

    peer_connection.on_data_channel(Box::new(move |d: Arc<RTCDataChannel>| {
        let sender_container = sender_container.clone();
        let d_clone = d.clone();
        
        d_clone.clone().on_open(Box::new(move || {
            let d_clone2 = d_clone.clone();
            let sender_container = sender_container.clone();
            Box::pin(async move {
                match d_clone2.detach().await {
                    Ok(raw) => {
                        // Take the sender from the Mutex
                        let mut guard = sender_container.lock().await;
                        if let Some(sender) = guard.take() {
                            let _ = sender.send(raw);
                        }
                    },
                    Err(e) => eprintln!("Failed to detach server data channel: {}", e),
                }
            })
        }));

        Box::pin(async {})
    }));

    let offer_sdp = serde_json::from_str::<RTCSessionDescription>(&offer.description)?;
    peer_connection.set_remote_description(offer_sdp).await?;

    let answer = peer_connection.create_answer(None).await?;
    peer_connection.set_local_description(answer).await?;

    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    let _ = gather_complete.recv().await;

    let local_desc = peer_connection
        .local_description()
        .await
        .ok_or_else(|| anyhow!("No local description"))?;
    let json_str = serde_json::to_string(&local_desc)?;

    send_reply_to_offer(offer, &json_str, signaling_tx).await;

    let detached_data_channel = detached_receiver.await?;
    let minecraft_stream = connect_to_local_server(minecraft_server).await;

    proxy_traffic(detached_data_channel, minecraft_stream).await?;
    Ok(())
}


async fn run_client_proxy(signaling_host: &str, id: &str) -> Result<()> {
    let certificate = generate_certificate().await?;
    let reply_manager = Arc::new(ResponseManager::new());

    let (signaling_tx, mut offer_reply_stream) = connect_to_signaling_server(signaling_host)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to signaling server: {}", e))?;

    register(id, signaling_tx.clone()).await;

    let reply_manager_clone = reply_manager.clone();
    tokio::spawn(async move {
        while let Some(offer_reply) = offer_reply_stream.next().await {
            if offer_reply.r#type == "reply" {
                reply_manager_clone.handle_response(offer_reply.number, offer_reply).await;
            } else {
                eprintln!("Unexpected message type: {}", offer_reply.r#type);
            }
        }
        eprintln!("Disconnected from signaling server");
    });

    listen_for_minecraft_client_connections("127.0.0.1:25565", {
        let signaling_tx = signaling_tx.clone();
        let certificate = certificate.clone();
        let reply_manager = reply_manager.clone();
        move |stream, _| {
            let signaling_tx = signaling_tx.clone();
            let certificate = certificate.clone();
            let reply_manager = reply_manager.clone();
            async move {
                if let Err(e) = handle_client_connection(stream, signaling_tx, &certificate, reply_manager).await {
                    eprintln!("Error handling client connection: {}", e);
                }
            }
        }
    }).await;

    Ok(())
}

async fn handle_client_connection(
    minecraft_stream: TcpStream,
    signaling_tx: SocketTx,
    certificate: &RTCCertificate,
    reply_manager: Arc<ResponseManager<OfferReply>>,
) -> Result<()> {
    let peer_connection = create_peer_connection(certificate.clone()).await?;

    let (detached_sender, detached_receiver) = tokio::sync::oneshot::channel();
    let sender_container = Arc::new(Mutex::new(Some(detached_sender)));

    let data_channel = peer_connection
        .create_data_channel(
            "minecraft",
            Some(RTCDataChannelInit {
                ordered: Some(true),
                ..Default::default()
            }),
        )
        .await?;

    data_channel.clone().on_open(Box::new(move || {
        let data_channel_clone = data_channel.clone();
        let sender_container = sender_container.clone();
        Box::pin(async move {
            match data_channel_clone.detach().await {
                Ok(raw) => {
                    // Take the sender from the Mutex
                    let mut guard = sender_container.lock().await;
                    if let Some(sender) = guard.take() {
                        let _ = sender.send(raw);
                    }
                },
                Err(e) => eprintln!("Failed to detach client data channel: {}", e),
            }
        })
    }));

    let offer = peer_connection.create_offer(None).await?;
    peer_connection.set_local_description(offer).await?;

    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    let _ = gather_complete.recv().await;

    let local_desc = peer_connection
        .local_description()
        .await
        .ok_or_else(|| anyhow!("No local description"))?;
    let json_str = serde_json::to_string(&local_desc)?;

    let offer_number = rand::random::<u32>();
    let reply_receiver = reply_manager.wait_for_response(offer_number).await;

    send_offer(
        OfferReply {
            r#type: "offer".to_string(),
            id: "client".to_string(),
            to: "testserver".to_string(),
            number: offer_number,
            description: json_str,
        },
        signaling_tx,
    ).await;

    let reply = reply_receiver.await?;
    let answer = serde_json::from_str::<RTCSessionDescription>(&reply.description)?;
    peer_connection.set_remote_description(answer).await?;

    let detached_data_channel = detached_receiver.await?;
    proxy_traffic(detached_data_channel, minecraft_stream).await?;

    Ok(())
}

async fn proxy_traffic(
    data_channel: Arc<webrtc::data::data_channel::DataChannel>,
    tcp_stream: TcpStream,
) -> Result<()> {
    let (mut tcp_read, mut tcp_write) = tokio::io::split(tcp_stream);
    let data_channel_clone = data_channel.clone();

    let read_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match data_channel_clone.read(&mut buf).await {
                Ok(n) => {
                    if let Err(e) = tcp_write.write_all(&buf[..n]).await {
                        eprintln!("TCP write error: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Data channel read error: {}", e);
                    break;
                }
            }
        }
    });

    let write_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(n) if n == 0 => break,
                Ok(n) => {
                    if let Err(e) = data_channel.write(&Bytes::copy_from_slice(&buf[..n])).await {
                        eprintln!("Data channel write error: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("TCP read error: {}", e);
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = read_task => {},
        _ = write_task => {},
    }

    Ok(())
}

async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}

async fn create_peer_connection(certificate: RTCCertificate) -> Result<Arc<RTCPeerConnection>> {
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
        ice_servers: vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        certificates: vec![certificate],
        ..Default::default()
    };

    Ok(Arc::new(api.new_peer_connection(config).await?))
}