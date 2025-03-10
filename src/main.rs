// #![feature(duration_constructors)]

use crate::client::start_client_proxy;
use crate::server::start_server_proxy;
use anyhow::Result;
use std::time::SystemTime;
use webpki::types::CertificateDer;
use x509_parser::prelude::{FromDer, X509Certificate};

mod client;
mod general;
mod log_on_drop;
mod p2p_helper;
mod parse_server;
mod register;
mod reply_manager;
mod server;
mod crypto; // Add this line

// pub const SIGNALING_SERVER: &str = "http://34.75.203.169:5100";
pub const SIGNALING_SERVER: &str = "ws://127.0.0.1:5100";

fn get_certificate_expiry(cert: &CertificateDer<'static>) -> Result<SystemTime> {
    let (_, parsed_cert) = X509Certificate::from_der(cert).ok().unwrap();
    let validity = parsed_cert.validity();
    Ok(SystemTime::from(validity.not_after.to_datetime()))
}

#[tokio::main]
async fn main() {
    // let (root_certified_key, root_age_key) = crypto::create_root().await.unwrap();
    let root_certified_key = crypto::load_root().await.unwrap();

    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";
    let id = std::env::args().nth(2).expect("Provide an ID");

    let (user_certified_key, user_age_key) = crypto::create_user(id.clone(), &root_certified_key).await.unwrap();
    let user_rtc_cert = crypto::load_user(id.clone()).await.unwrap();
    
    // crypto::parse_cert(user_certified_key.cert.der()).await.unwrap();

    if is_client {
        let port = std::env::args().nth(3).unwrap_or("25565".to_string()).parse::<u16>().expect("Port must be a number");
        start_client_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert, root_certified_key.cert.der().to_vec()).await;
    } else {
        let port = std::env::args().nth(3).expect("Provide a port on which a Minecraft server runs").parse::<u16>().expect("Port must be a number");
        start_server_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert).await;
    }
}