use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct MinecraftListener {
    inner: TcpListener,
}

impl MinecraftListener {
    /// Creates a new Minecraft listener bound to the specified address
    #[instrument]
    pub async fn bind<A: tokio::net::ToSocketAddrs + std::fmt::Debug>(
        addr: A,
    ) -> tokio::io::Result<Self> {
        let inner = TcpListener::bind(&addr).await?;
        info!("Starting Minecraft client adapter on {:?}", addr);
        Ok(Self { inner })
    }

    /// Accepts a new incoming connection with Minecraft-specific setup
    pub async fn accept(&self) -> tokio::io::Result<(TcpStream, SocketAddr)> {
        let (stream, addr) = self.inner.accept().await
            .map_err(|e| {
                tracing::error!(error = ?e, "Failed accepting Minecraft connection");
                e
            })?;

        stream.set_nodelay(true)
            .map_err(|e| {
                tracing::error!(error = ?e, "Failed setting TCP_NODELAY");
                e
            })?;

        info!("Minecraft client connected from {}", addr);
        Ok((stream, addr))
    }
}