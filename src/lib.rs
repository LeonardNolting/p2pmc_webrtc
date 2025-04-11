use std::sync::Arc;
use crate::util::logging::start_logger;
use anyhow::{Result};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::session::Session;
use crate::core::proxies::server::jude_server;
use crate::infra::storage::{get_server, run_server};

pub(crate) mod util;
pub(crate) mod cli;
pub(crate) mod core;
pub(crate) mod infra;
pub(crate) mod crypto;

/*#[tokio::main]
async fn main() -> Result<()> {
    start_logger()?;

    cli().await?;
    
    Ok(())
}*/

pub(crate) async fn share_running_server(name: PeerId, session: Arc<Session>, address: &str) -> Result<()> {
    jude_server(name, session, address).await?;
    
    Ok(())
}

pub(crate) async fn download_and_run_server(name: PeerId, session: Arc<Session>, address: &str) -> Result<()> {
    get_server(name.clone()).await?;
    
    run_server(name.clone()).await?;
    
    // TODO close server whenever something crashes?!
    jude_server(name, session, address).await?;
    
    Ok(())
}
