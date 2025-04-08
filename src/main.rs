use crate::cli::cli;
use crate::util::logging::start_logger;
use anyhow::{Result};
use util::response_manager::ResponseManager;
use webrtc::peer_connection::certificate::RTCCertificate;

mod util;
mod cli;
mod core;
mod infra;
mod crypto;

#[tokio::main]
async fn main() -> Result<()> {
    start_logger()?;

    cli().await?;
    
    Ok(())
}

pub async fn generate_certificate() -> Result<RTCCertificate> {
    let keypair = rcgen::KeyPair::generate()?;
    let cert = RTCCertificate::from_key_pair(keypair)?;
    Ok(cert)
}
