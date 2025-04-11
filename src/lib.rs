use std::sync::Arc;
use crate::util::logging::start_logger;
use anyhow::{Result};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::session::Session;
use crate::core::proxies::server::jude_server;
use crate::infra::storage::{get_server, run_server};

pub mod util;
pub mod cli;
pub mod core;
pub mod infra;
pub mod crypto;

/*#[tokio::main]
async fn main() -> Result<()> {
    start_logger()?;

    cli().await?;
    
    Ok(())
}*/

pub async fn share_running_server(name: PeerId, session: Arc<Session>, address: &str) -> Result<()> {
    jude_server(name, session, address).await?;
    
    Ok(())
}

pub async fn download_and_run_server(name: PeerId, session: Arc<Session>, address: &str) -> Result<()> {
    get_server(name.clone()).await?;
    
    run_server(name.clone()).await?;
    
    // TODO close server whenever something crashes?!
    jude_server(name, session, address).await?;
    
    Ok(())
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
