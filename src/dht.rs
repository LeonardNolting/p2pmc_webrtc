use std::sync::Arc;
use pkarr::dns::rdata::{RData, TXT};
use pkarr::dns::Name;
use pkarr::{Client, Keypair, SignedPacket};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Deterministically derives an Ed25519 Keypair from an arbitrary string.
fn derive_keypair_from_name(name: &str) -> Keypair {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let seed: [u8; 32] = hasher.finalize().into();
    Keypair::from_secret_key(&seed)
}

/// Publishes an Iroh ticket to the DHT and spawns a background task to republish it.
///
/// Returns `Ok(())` immediately after the *first* successful publish.
pub async fn publish_iroh_mapping(
    client: Client, // TODO later: pass by value (it clones cheaply via internal Arc) for the background task
    name: String,
    ticket: String,
    cancel_token: CancellationToken,
    dns_ttl: Option<u32>,
    interval_seconds: Option<u64>,
) -> Result<(), String> {
    let ttl = dns_ttl.unwrap_or(1);

    let interval = Duration::from_secs(interval_seconds.unwrap_or(3600));

    let keypair = derive_keypair_from_name(&name);

    // Create and sign the packet once
    // This bakes the current timestamp into the sequence number.
    let signed_packet = SignedPacket::builder()
        .txt(
            Name::try_from("_iroh").map_err(|e| e.to_string())?,
            TXT::try_from(ticket.as_str()).map_err(|e| e.to_string())?,
            ttl,
        )
        .sign(&keypair)
        .map_err(|e| e.to_string())?;

    client
        .publish(&signed_packet, None)
        .await
        .map_err(|e| e.to_string())?;
    info!("Successfully published ticket for '{}', public key: {}, value: {}", name, keypair.public_key(), ticket.as_str());

    let packet_to_republish = signed_packet.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Wait for the cancellation token from Flutter
                _ = cancel_token.cancelled() => {
                    println!("Republishing task for '{}' cancelled.", name);
                    break;
                }
                // Or wait for the interval to pass
                _ = tokio::time::sleep(interval) => {
                    // FIX: We publish the EXACT SAME packet. We DO NOT rebuild or resign.
                    // This keeps the record alive on the DHT without bumping the sequence
                    // number, which prevents it from accidentally overwriting future mutations!
                    // TODO check if this works even when the packet timestamp is not updated? 10 hour check?
                    if let Err(e) = client.publish(&packet_to_republish, None).await {
                        eprintln!("Background republish failed for {}: {}", name, e);
                    } else {
                        println!("Successfully republished '{}' to keep it alive on DHT", name);
                    }
                }
            }
        }
    });

    Ok(())
}

/// Looks up an Iroh ticket in the DHT associated with a human-readable name.
pub async fn lookup_iroh_mapping(
    client: Client,
    name: String,
) -> Result<Option<String>, String> {
    let public_key = derive_keypair_from_name(&name).public_key();

    // Resolve returns None if no packet is found, which we safely pass up to Dart
    if let Some(packet) = client.resolve(&public_key).await {
        for record in packet.resource_records("_iroh") {
            if let RData::TXT(txt) = &record.rdata {
                if let Ok(ticket) = String::try_from(txt.clone()) {
                    info!("Retrieved ticket for {} (public key: {}): {}", name, public_key.to_string(), ticket);
                    return Ok(Some(ticket));
                }
            }
        }
    }

    Ok(None)
}

pub fn create_pkarr_client() -> Result<Client, String> {
    Client::builder()
        .minimum_ttl(1)
        .maximum_ttl(24 * 60 * 60)
        .build().map_err(|e| e.to_string())
}