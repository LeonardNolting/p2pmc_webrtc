use std::io::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

// -----------------------------------------------------------------------------
// VarInt helpers
// -----------------------------------------------------------------------------

/// Encodes a VarInt into a Vec<u8>.
pub fn encode_varint(mut value: i32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5);
    loop {
        let mut byte = (value & 0x7F) as u8;
        value = ((value as u32) >> 7) as i32;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
    buf
}

/// Decodes a VarInt from a byte slice, returning (value, bytes_consumed).
pub fn decode_varint(buf: &[u8]) -> Option<(i32, usize)> {
    let mut result = 0i32;
    let mut shift = 0;
    for (i, &byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7F) as i32) << shift;
        if (byte & 0x80) == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift >= 35 {
            return None; // overflow
        }
    }
    None
}

/// Encodes a UTF-8 string as a Minecraft protocol String (VarInt length + UTF-8 bytes).
pub fn encode_mc_string(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut buf = encode_varint(bytes.len() as i32);
    buf.extend_from_slice(bytes);
    buf
}

// -----------------------------------------------------------------------------
// Protocol version threshold
// NBT text components were introduced in 1.20.3 (protocol version 765).
// -----------------------------------------------------------------------------

const NBT_TEXT_COMPONENT_MIN_PROTOCOL: i32 = 765;

// -----------------------------------------------------------------------------
// Text component encoding
//
// Pre-1.20.3:  JSON string, encoded as a Minecraft String (varint length + UTF-8)
//              e.g. {"text":"Server not found"}
//
// 1.20.3+:     Named Binary Tag (NBT), specifically a TAG_Compound containing
//              a TAG_String named "text".
//
// Network NBT wire format for a text component { "text": "<msg>" }:
//
// Minecraft's network NBT (used in packets since 1.20.3) omits the leading
// type byte and root compound name that appear in file/disk NBT.  The payload
// starts directly with the first entry inside the compound:
//
//   0x08                       — TAG_String type  (first entry in the compound)
//   0x00 0x04                  — name length = 4  (big-endian u16)
//   0x74 0x65 0x78 0x74        — "text"
//   hi lo                      — value length (big-endian u16)
//   <bytes>                    — UTF-8 value
//   0x00                       — TAG_End (closes the compound)
//
// Do NOT prepend 0x0A (TAG_Compound) or a root name — that is disk NBT format
// and will cause the client to misparse the packet.
// -----------------------------------------------------------------------------

fn encode_text_component_nbt(text: &str) -> Vec<u8> {
    let text_bytes = text.as_bytes();
    let text_len = text_bytes.len() as u16;

    let key = b"text";
    let key_len = key.len() as u16;

    let mut buf = Vec::new();
    // No leading TAG_Compound type byte or root name — network NBT only.

    // TAG_String entry: "text" -> message
    buf.push(0x08); // TAG_String
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&text_len.to_be_bytes());
    buf.extend_from_slice(text_bytes);

    // TAG_End (closes the implicit root compound)
    buf.push(0x00);
    buf
}

fn encode_text_component_json(text: &str) -> Vec<u8> {
    // Escape quotes in the text for JSON safety
    let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
    let json = format!(r#"{{"text":"{}"}}"#, escaped);
    encode_mc_string(&json)
}

// -----------------------------------------------------------------------------
// Login Disconnect packet (clientbound, Login state, packet ID 0x00)
//
// Wire format (no compression active at this point):
//   [VarInt: total packet length] [VarInt: packet ID = 0x00] [reason payload]
//
// "reason payload" differs by version:
//   < 1.20.3  → Minecraft String  (VarInt length-prefixed UTF-8 JSON)
//   ≥ 1.20.3  → NBT compound
// -----------------------------------------------------------------------------

pub fn build_login_disconnect(message: &str, protocol_version: i32) -> Vec<u8> {
    let packet_id = encode_varint(0x00);
    let reason = if protocol_version >= NBT_TEXT_COMPONENT_MIN_PROTOCOL {
        encode_text_component_nbt(message)
    } else {
        encode_text_component_json(message)
    };

    let mut payload = packet_id;
    payload.extend_from_slice(&reason);

    let mut packet = encode_varint(payload.len() as i32);
    packet.extend_from_slice(&payload);
    packet
}

// -----------------------------------------------------------------------------
// Peek-based protocol version extraction
//
// We re-parse the already-peeked handshake buffer to retrieve the protocol
// version so we can branch on NBT vs JSON encoding.
//
// Handshake packet layout (all VarInts unless noted):
//   [packet length] [packet ID = 0x00] [protocol version] [server address string]
//   [server port: u16 big-endian] [next state VarInt]
// -----------------------------------------------------------------------------

/// Extract the protocol version from the raw peeked bytes of a Handshake packet.
/// Returns None if the buffer is too short or malformed.
pub fn parse_protocol_version(buf: &[u8]) -> Option<i32> {
    let mut pos = 0;

    // Skip packet length
    let (_, n) = decode_varint(&buf[pos..])?;
    pos += n;

    // Skip packet ID
    let (_, n) = decode_varint(&buf[pos..])?;
    pos += n;

    // Read protocol version
    let (version, _) = decode_varint(&buf[pos..])?;
    Some(version)
}

// -----------------------------------------------------------------------------
// High-level helper: send a Login Disconnect and flush.
//
// Call this *after* parse_server has already peeked the stream (so the protocol
// version is available from the same buffer).  The write side only needs the
// TcpStream and can be called regardless of whether we actually consumed any
// bytes — the client transitions its own state to Login as soon as it *sends*
// the Handshake, so it will correctly interpret a Login Disconnect reply.
// -----------------------------------------------------------------------------

/// Send a Login Disconnect packet to the client with a human-readable message,
/// then shut down the write half.  `protocol_version` should come from
/// `parse_protocol_version`; if unknown, pass 0 to use the legacy JSON format
/// (safe for very old clients; modern ones tolerate a graceful close anyway).
pub async fn send_login_disconnect(
    stream: &mut TcpStream,
    message: &str,
    protocol_version: i32,
) -> std::io::Result<()> {
    let packet = build_login_disconnect(message, protocol_version);
    // Best-effort: ignore write errors — the important thing is we tried.
    let write_timeout = Duration::from_millis(500);
    let _ = timeout(write_timeout, stream.write_all(&packet)).await;
    let _ = stream.shutdown().await;
    Ok(())
}