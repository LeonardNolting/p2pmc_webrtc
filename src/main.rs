#![feature(duration_constructors)]

use std::sync::Arc;
use crate::client::start_client_proxy;
use crate::server::start_server_proxy;
use rcgen::{Certificate, CertificateParams, CertifiedKey, CustomExtension, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType};
use std::time::Duration;
use age::Identity;
use age::x25519::Recipient;
use time::OffsetDateTime;
use anyhow::Result;
use tokio::fs::{read, read_to_string, write};

mod client;
mod general;
mod log_on_drop;
mod p2p_helper;
mod parse_server;
mod register;
mod reply_manager;
mod server;

// pub const SIGNALING_SERVER: &str = "http://34.75.203.169:5100";
pub const SIGNALING_SERVER: &str = "ws://127.0.0.1:5100";

#[tokio::main]
async fn main() {
    // let root_certified_key = create_root_certificate().await.unwrap();
    let root_certified_key = load_root_certificate().await.unwrap();
    println!("Root public key: {}", root_certified_key.key_pair.public_key_pem());
    let user_certified_key = create_user("Munkel_".to_string(), &root_certified_key).unwrap();

    /*
    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";
    let id = std::env::args().nth(2).expect("Provide an ID");

    if is_client {
        let port = std::env::args().nth(3).unwrap_or("25565".to_string()).parse::<u16>().expect("Port must be a number");
        start_client_proxy(SIGNALING_SERVER, id.as_str(), port, Arc::new(user_certified_key)).await;
    } else {
        let port = std::env::args().nth(3).expect("Provide a port on which a Minecraft server runs").parse::<u16>().expect("Port must be a number");
        start_server_proxy(SIGNALING_SERVER, id.as_str(), port, Arc::new(root_certified_key)).await;
    }*/
}

async fn load_root_certificate() -> Result<CertifiedKey> {
    let root_certificate = read("root.cer").await?;
    let root_private_key = read("root.key").await?;

    let key_pair = KeyPair::try_from(root_private_key)?;

    let certified_key = CertifiedKey {
        cert: CertificateParams::from_ca_cert_der(&root_certificate.into()).unwrap().self_signed(&key_pair)?,
        key_pair
    };

    Ok(certified_key)
}

async fn create_root_certificate() -> Result<CertifiedKey> {
    let mut params = CertificateParams::default();

    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "P2PMC Root CA");

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = params.not_before + Duration::from_days(365 * 1000);

    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    write("root.cer", cert.der()).await?;
    write("root.key", key_pair.serialize_der()).await?;

    Ok(CertifiedKey { cert, key_pair })
}

fn create_user(user: String, issuer: &CertifiedKey) -> Result<(impl Identity, CertifiedKey)> {
    let age_key = age::x25519::Identity::generate();
    let age_public_key = age_key.to_public();
    
    let certified_key = create_user_certificate(user, issuer, age_public_key)?;
    
    Ok((age_key, certified_key))
}

fn create_user_certificate(user: String, issuer: &CertifiedKey, age_public_key: Recipient) -> Result<CertifiedKey> {
    let mut params = CertificateParams::default();

    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        format!("user:{user}")
    );
    
    // See https://serverfault.com/questions/551477/is-there-reserved-oid-space-for-internal-enterprise-cas
    // and https://oid-base.com/cgi-bin/display?oid=2.25&submit=Display&action=display
    // and https://www.itu.int/itu-t/recommendations/rec.aspx?rec=X.667
    let uuid = "236713699648986742819063550858365452248";
    let uuid_u128 = str::parse::<u128>(uuid).unwrap();
    let high_bits = (uuid_u128 >> 64) as u64;
    let low_bits = (uuid_u128 & 0xFFFFFFFFFFFFFFFF) as u64;
    // 2.25.236713699648986742819063550858365452248
    let age_public_key_oid: &[u64] = &[2, 25, high_bits, low_bits];
    params.custom_extensions.push(CustomExtension::from_oid_content(age_public_key_oid, age_public_key.to_string().into_bytes()));

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = params.not_before + Duration::from_days(365 * 100);

    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
    ];

    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    params.is_ca = IsCa::ExplicitNoCa;

    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, &issuer.cert, &issuer.key_pair)?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    println!("User Certificate:\n{}", cert_pem);
    println!("User Key:\n{}", key_pem);

    Ok(CertifiedKey { cert, key_pair })
}
