#![feature(duration_constructors)]

use crate::client::start_client_proxy;
use crate::server::start_server_proxy;
use age::secrecy::ExposeSecret;
use age::x25519::{Identity, Recipient};
use anyhow::Result;
use rcgen::{
    CertificateParams, CertifiedKey, CustomExtension, ExtendedKeyUsagePurpose,
    Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use tokio::fs::{read, read_to_string, write};
use webpki::types::{CertificateDer, ServerName, UnixTime};
use webpki::KeyUsage;
use webrtc::dtls::crypto::CryptoPrivateKey;
use webrtc::peer_connection::certificate::RTCCertificate;
use x509_parser::prelude::{FromDer, X509Certificate};

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

fn get_certificate_expiry(cert: &CertificateDer<'static>) -> Result<SystemTime> {
    let (_, parsed_cert) = X509Certificate::from_der(cert).ok().unwrap();
    let validity = parsed_cert.validity();
    Ok(SystemTime::from(validity.not_after.to_datetime()))
}

#[tokio::main]
async fn main() {
    // let (root_certified_key, root_age_key) = create_root().await.unwrap();
    let root_certified_key = load_root().await.unwrap();

    let is_client = std::env::args().nth(1).expect("Server or client?") == "client";
    let id = std::env::args().nth(2).expect("Provide an ID");

    let (user_certified_key, user_age_key) = create_user(id.clone(), &root_certified_key).await.unwrap();
    let user_rtc_cert = load_user(id.clone()).await.unwrap();
    
    // parse_cert(user_certified_key.cert.der()).await.unwrap();

    if is_client {
        let port = std::env::args().nth(3).unwrap_or("25565".to_string()).parse::<u16>().expect("Port must be a number");
        start_client_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert, root_certified_key.cert.der().to_vec()).await;
    } else {
        let port = std::env::args().nth(3).expect("Provide a port on which a Minecraft server runs").parse::<u16>().expect("Port must be a number");
        start_server_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert).await;
    }
}

async fn parse_cert(cert: &[u8]) -> Result<Identity> {
    let (_rem, cert) = X509Certificate::from_der(cert)?;

    let age_public_key = cert.extensions().iter().find(|ext| {
        ext.oid == "2.25.10".parse().unwrap()
    }).unwrap().value;
    let age_public_key = str::from_utf8(age_public_key)?;

    println!("Parsed age public key: {}", age_public_key);

    Ok(Identity::from_str(age_public_key).unwrap())
}

pub async fn validate_is_peer(
    peer: String,
    cert: &CertificateDer<'_>,
    root_cert: &CertificateDer<'_>,
) -> Result<()> {
    let trust_anchor = webpki::anchor_from_trusted_cert(root_cert)?;
    let trust_anchors = &[trust_anchor];

    let cert = webpki::EndEntityCert::try_from(cert)?;

    cert.verify_for_usage(
        &webpki::ALL_VERIFICATION_ALGS, // Or specify supported algorithms
        trust_anchors,
        &[], // Intermediate certificates, if any
        UnixTime::now(),
        KeyUsage::client_auth(), // Use server_auth for servers, client_auth for clients
        None,                    // Revocation options
        None,                    // Path verification callback
    )?;

    cert.verify_is_valid_for_subject_name(&ServerName::try_from(user_to_domain(&peer))?)?;

    println!("Is valid peer");

    Ok(())
}

async fn load_user(user: String) -> Result<RTCCertificate> {
    let user_certificate = read(format!("{}.cer", user)).await?;
    let cert: CertificateDer<'static> = user_certificate.into();

    let user_private_key = read(format!("{}.key", user)).await?;
    let key_pair = KeyPair::try_from(user_private_key)?;

    let expires = get_certificate_expiry(&cert)?;

    Ok(RTCCertificate::from_existing(webrtc::dtls::crypto::Certificate {
        certificate: vec![cert],
        private_key: CryptoPrivateKey::try_from(&key_pair)?,
    }, expires))
}

async fn load_root() -> Result<CertifiedKey> {
    let root_certificate = read("root.cer").await?;
    let root_private_key = read("root.key").await?;
    let root_age_key = read_to_string("root.age.key").await?;

    let age = Identity::from_str(&root_age_key).unwrap();

    println!("Root age public key: {}", age.to_public());

    let key_pair = KeyPair::try_from(root_private_key)?;

    let certified_key = CertifiedKey {
        cert: CertificateParams::from_ca_cert_der(&root_certificate.into())
            .unwrap()
            .self_signed(&key_pair)?,
        key_pair,
    };

    Ok(certified_key)
}

async fn create_root() -> Result<(CertifiedKey, Identity)> {
    let age_key = Identity::generate();

    write(
        "root.age.key",
        age_key.to_string().expose_secret().as_bytes(),
    )
    .await?;

    let age_public_key = age_key.to_public();

    println!("Root age key: {}", age_key.to_string().expose_secret());
    println!("Root age public key: {}", age_public_key.to_string());

    let certified_key = create_root_certificate(age_public_key).await?;

    Ok((certified_key, age_key))
}

async fn create_root_certificate(age_public_key: Recipient) -> Result<CertifiedKey> {
    let mut params = CertificateParams::default();

    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "P2PMC Root CA");

    // See https://serverfault.com/questions/551477/is-there-reserved-oid-space-for-internal-enterprise-cas
    // and https://oid-base.com/cgi-bin/display?oid=2.25&submit=Display&action=display
    // and https://www.itu.int/itu-t/recommendations/rec.aspx?rec=X.667
    /* let uuid: u128 = 236713699648986742819063550858365452248;
    let high_bits = (uuid >> 64) as u64;
    let low_bits = (uuid & 0xFFFFFFFFFFFFFFFF) as u64;
    // 2.25.236713699648986742819063550858365452248
    let age_public_key_oid: &[u64] = &[2, 25, high_bits, low_bits]; */
    let age_public_key_oid = &[2, 25, 10];
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            age_public_key_oid,
            age_public_key.to_string().into_bytes(),
        ));

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = params.not_before + Duration::from_days(365 * 1000);

    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    write("root.cer", cert.der()).await?;
    write("root.key", key_pair.serialize_der()).await?;

    Ok(CertifiedKey { cert, key_pair })
}

async fn create_user(user: String, issuer: &CertifiedKey) -> Result<(CertifiedKey, Identity)> {
    let age_key = Identity::generate();

    write(
        format!("{}.age.key", user),
        age_key.to_string().expose_secret().as_bytes(),
    )
    .await?;

    let age_public_key = age_key.to_public();

    println!("User age key: {}", age_key.to_string().expose_secret());
    println!("User age public key: {}", age_public_key.to_string());

    let certified_key = create_user_certificate(user, issuer, age_public_key).await?;

    Ok((certified_key, age_key))
}

async fn create_user_certificate(
    user: String,
    issuer: &CertifiedKey,
    age_public_key: Recipient,
) -> Result<CertifiedKey> {
    let mut params = CertificateParams::default();

    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("user:{user}"));

    params
        .subject_alt_names
        .push(SanType::DnsName(Ia5String::try_from(user_to_domain(
            &user,
        ))?));

    // See https://serverfault.com/questions/551477/is-there-reserved-oid-space-for-internal-enterprise-cas
    // and https://oid-base.com/cgi-bin/display?oid=2.25&submit=Display&action=display
    // and https://www.itu.int/itu-t/recommendations/rec.aspx?rec=X.667
    /* let uuid: u128 = 236713699648986742819063550858365452248;
    let high_bits = (uuid >> 64) as u64;
    let low_bits = (uuid & 0xFFFFFFFFFFFFFFFF) as u64;
    // 2.25.236713699648986742819063550858365452248
    let age_public_key_oid: &[u64] = &[2, 25, high_bits, low_bits]; */
    let age_public_key_oid = &[2, 25, 10];
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            age_public_key_oid,
            age_public_key.to_string().into_bytes(),
        ));

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = params.not_before + Duration::from_days(365 * 100);

    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::ContentCommitment,
        KeyUsagePurpose::DataEncipherment,
    ];

    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

    params.is_ca = IsCa::ExplicitNoCa;

    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, &issuer.cert, &issuer.key_pair)?;

    write(format!("{}.cer", user), cert.der()).await?;
    write(format!("{}.key", user), key_pair.serialize_der()).await?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    println!("User Certificate:\n{}", cert_pem);
    println!("User Key:\n{}", key_pem);

    Ok(CertifiedKey { cert, key_pair })
}

fn user_to_domain(user: &String) -> String {
    format!("{}.users.p2pmc.internal", user)
}
