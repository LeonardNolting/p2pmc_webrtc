use age::secrecy::ExposeSecret;
use age::x25519::{Identity, Recipient};
use anyhow::Result;
use rcgen::{
    CertificateParams, CertifiedKey, CustomExtension, ExtendedKeyUsagePurpose,
    Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;
use tokio::fs::{read, read_to_string, write};
use webpki::types::{CertificateDer, ServerName, UnixTime};
use webpki::KeyUsage;
use webrtc::dtls::crypto::CryptoPrivateKey;
use webrtc::peer_connection::certificate::RTCCertificate;
use x509_parser::prelude::{FromDer, X509Certificate};

pub(crate) fn get_certificate_expiry(cert: &CertificateDer<'static>) -> Result<SystemTime> {
    let (_, parsed_cert) = X509Certificate::from_der(cert).ok().unwrap();
    let validity = parsed_cert.validity();
    Ok(SystemTime::from(validity.not_after.to_datetime()))
}

pub(crate) async fn parse_cert(cert: &[u8]) -> Result<Identity> {
    let (_rem, cert) = X509Certificate::from_der(cert)?;

    let age_public_key = cert.extensions().iter().find(|ext| {
        ext.oid == "2.25.10".parse().unwrap()
    }).unwrap().value;
    let age_public_key = std::str::from_utf8(age_public_key)?;

    println!("Parsed age public key: {}", age_public_key);

    Ok(Identity::from_str(age_public_key).unwrap())
}

pub(crate) async fn validate_is_peer(
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

pub(crate) async fn load_user(user: String) -> Result<RTCCertificate> {
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

fn user_to_domain(user: &String) -> String {
    format!("{}.users.jude.internal", user)
}