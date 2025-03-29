use std::future::Future;
use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};
use tracing::info;

#[tracing::instrument]
pub async fn connect_to_local_minecraft_server(url: &str) -> TcpStream {
    // set up a connection to the Minecraft server
    info!("Connecting to Minecraft server at {url}");
    let stream = TcpStream::connect(url)
        .await
        .expect(&format!("Couldn't connect to Minecraft server under {url}"));
    stream.set_nodelay(true).unwrap();
    stream
}

#[tracing::instrument(skip(on_connect, url))]
pub async fn listen_for_minecraft_client_connections<
    Fut: Future,
    F: (Fn(TcpStream, SocketAddr) -> Fut) + Send + 'static,
>(
    url: &str,
    on_connect: F,
) {
    info!("Starting Minecraft client adapter under {url}");
    let listener = TcpListener::bind(url).await.unwrap();
    info!("Listening for TCP connections from Minecraft clients under {url}");
    loop {
        let (stream, address) = listener
            .accept()
            .await
            .expect(&format!("Couldn't connect to Minecraft client under {url}"));
        stream.set_nodelay(true).unwrap();
        
        info!("Minecraft client connected from {address}");

        on_connect(stream, address).await;
    }
}