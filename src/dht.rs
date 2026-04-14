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
    client: Arc<Client>, // TODO later: pass by value (it clones cheaply via internal Arc) for the background task
    name: String,
    ticket: String,
    cancel_token: CancellationToken,
    ttl_seconds: Option<u32>,
    interval_seconds: Option<u64>,
) -> Result<(), String> {
    // Kademlia DHT nodes generally drop records after 2 hours.
    // We default to a 2-hour TTL and a 1-hour republish interval.
    let ttl = ttl_seconds.unwrap_or(7200);
    let interval = Duration::from_secs(interval_seconds.unwrap_or(3600));

    let keypair = derive_keypair_from_name(&name);

    // 1. Create the initial packet
    // FIX: Use explicit TryFrom calls and convert the String to a &str
    let signed_packet = SignedPacket::builder()
        .txt(
            Name::try_from("_iroh").map_err(|e| e.to_string())?,
            TXT::try_from(ticket.as_str()).map_err(|e| e.to_string())?,
            ttl,
        )
        .sign(&keypair)
        .map_err(|e| e.to_string())?;

    // 2. Publish immediately so the caller knows it worked at least once
    client
        .publish(&signed_packet, None)
        .await
        .map_err(|e| e.to_string())?;
    info!("Successfully published ticket for '{}', public key: {}", name, keypair.public_key());

    // 3. Spawn the background republishing loop
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
                    // Rebuild and resign the packet (updates the timestamp)
                    // FIX: Apply the same TryFrom and .as_str() logic here
                    let packet_update = SignedPacket::builder()
                        .txt(
                            Name::try_from("_iroh").expect("Failed to parse Name"),
                            TXT::try_from(ticket.as_str()).expect("Failed to parse TXT"),
                            ttl
                        )
                        .sign(&keypair)
                        .expect("Failed to sign packet in background task");

                    if let Err(e) = client.publish(&packet_update, None).await {
                        // In a real app, you might want to log this to a file or tracing subscriber,
                        // rather than crashing the background thread.
                        eprintln!("Background republish failed for {}: {}", name, e);
                    } else {
                        println!("Successfully republished '{}'", name);
                    }
                }
            }
        }
    });

    Ok(())
}

/// Looks up an Iroh ticket in the DHT associated with a human-readable name.
pub async fn lookup_iroh_mapping(
    client: Arc<Client>,
    name: String,
) -> Result<Option<String>, String> {
    let public_key = derive_keypair_from_name(&name).public_key();

    // Resolve returns None if no packet is found, which we safely pass up to Dart
    if let Some(packet) = client.resolve(&public_key).await {
        for record in packet.resource_records("_iroh") {
            if let RData::TXT(txt) = &record.rdata {
                if let Ok(ticket) = String::try_from(txt.clone()) {
                    info!("Retrieved ticket for {}: {}", name, ticket);
                    return Ok(Some(ticket));
                }
            }
        }
    }

    Ok(None)
}

pub fn create_pkarr_client() -> Result<Client, String> {
    Client::builder().build().map_err(|e| e.to_string())
}