use tokio::net::TcpStream;
use tracing::{info, instrument};

#[derive(Debug)]
pub(crate) struct MinecraftConnector;

impl MinecraftConnector {
    /// Connects to a Minecraft server with proper configuration
    #[instrument]
    pub(crate) async fn connect<A: tokio::net::ToSocketAddrs + std::fmt::Debug>(
        addr: A,
    ) -> std::io::Result<TcpStream> {
        info!("Connecting to Minecraft server at {:?}", addr);

        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Failed to connect to Minecraft server");
                e
            })?;

        stream.set_nodelay(true)
            .map_err(|e| {
                tracing::error!(error = ?e, "Failed to set TCP_NODELAY");
                e
            })?;

        info!("Successfully connected to Minecraft server at {:?}", addr);
        Ok(stream)
    }
}