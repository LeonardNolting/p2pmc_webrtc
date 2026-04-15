use anyhow::Context;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::info;
use url::Url;

use crate::util::mc_disconnect::decode_varint;

// Re-export so callers don't need to import mc_disconnect directly for this type.
/// Everything extracted from the Minecraft Handshake packet that we need
/// both to route the connection and to send well-formed error replies.
pub struct HandshakeInfo {
    /// The subdomain component we route on (e.g. "myserver" from "myserver.jude.gg").
    pub server_id: String,
    /// Raw protocol version from the Handshake packet; used to pick NBT vs JSON
    /// encoding for Login Disconnect packets.
    pub protocol_version: i32,
    /// next_state field from the Handshake: 1 = Status, 2 = Login.
    /// We should only send Login Disconnect when this is 2.
    pub next_state: i32,
}

// -----------------------------------------------------------------------------
// Public entry point (replaces the old parse_server)
// -----------------------------------------------------------------------------

#[tracing::instrument(skip(stream))]
pub(crate) async fn parse_handshake(stream: &mut TcpStream) -> anyhow::Result<HandshakeInfo> {
    let handshake_timeout = Duration::from_millis(500);

    timeout(handshake_timeout, read_handshake(stream))
        .await
        .context("Handshake timed out")?
}

// -----------------------------------------------------------------------------
// Internal implementation
// -----------------------------------------------------------------------------

async fn read_handshake(stream: &mut TcpStream) -> anyhow::Result<HandshakeInfo> {
    // Peek so the bytes remain available for the peer once we start forwarding.
    // 300 bytes is plenty for a Handshake (server address is capped at 255 chars
    // by the protocol, plus VarInt overhead).
    let mut peek_buf = vec![0u8; 512];
    let bytes_read = stream.peek(&mut peek_buf).await?;

    if bytes_read == 0 {
        anyhow::bail!("Connection closed before receiving handshake");
    }

    let buf = &peek_buf[..bytes_read];
    parse_handshake_buf(buf)
}

/// Pure parsing of a raw handshake buffer — no I/O, fully testable.
fn parse_handshake_buf(buf: &[u8]) -> anyhow::Result<HandshakeInfo> {
    let mut pos = 0;

    // ---- packet length (VarInt, skip) ----
    let (_, n) = decode_varint(&buf[pos..])
        .ok_or_else(|| anyhow::anyhow!("Failed to read packet length"))?;
    pos += n;

    // ---- packet ID (VarInt, must be 0x00 for Handshake) ----
    let (packet_id, n) = decode_varint(&buf[pos..])
        .ok_or_else(|| anyhow::anyhow!("Failed to read packet ID"))?;
    pos += n;
    anyhow::ensure!(packet_id == 0x00, "Unexpected packet ID {packet_id:#04x}, expected Handshake (0x00)");

    // ---- protocol version (VarInt, keep!) ----
    let (protocol_version, n) = decode_varint(&buf[pos..])
        .ok_or_else(|| anyhow::anyhow!("Failed to read protocol version"))?;
    pos += n;

    // ---- server address (VarInt length + UTF-8) ----
    let (addr_len, n) = decode_varint(&buf[pos..])
        .ok_or_else(|| anyhow::anyhow!("Failed to read server address length"))?;
    pos += n;

    let addr_len = addr_len as usize;
    anyhow::ensure!(
        pos + addr_len <= buf.len(),
        "Server address truncated in peek buffer (need {} bytes, have {})",
        addr_len,
        buf.len() - pos
    );
    let server_address = String::from_utf8_lossy(&buf[pos..pos + addr_len]).into_owned();
    pos += addr_len;

    // ---- server port (u16, skip) ----
    pos += 2;

    // ---- next_state (VarInt) ----
    let (next_state, _) = decode_varint(&buf[pos..])
        .ok_or_else(|| anyhow::anyhow!("Failed to read next_state"))?;

    // ---- extract the routing subdomain ----
    // Strip the FML/Forge marker that some clients append (e.g. "host\x00FML2\x00")
    let clean_address = server_address
        .split('\x00')
        .next()
        .unwrap_or(&server_address);

    let domain = Url::parse(clean_address).map_or_else(
        |_| clean_address.to_string(),
        |url| {
            url.domain()
                .unwrap_or(clean_address)
                .to_string()
        },
    );

    // Reverse domain components and take the last (outermost) subdomain.
    // "myserver.jude.gg" → ["gg", "jude", "myserver"] → last = "myserver"
    let mut parts: Vec<&str> = domain.split('.').collect();
    parts.reverse();
    let server_id = parts
        .last()
        .ok_or_else(|| anyhow::anyhow!("Could not extract server ID from domain '{domain}'"))?
        .to_string();

    info!(
        protocol_version,
        server_id,
        next_state,
        "Parsed handshake"
    );

    Ok(HandshakeInfo {
        server_id,
        protocol_version,
        next_state,
    })
}