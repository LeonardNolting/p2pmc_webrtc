use std::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
// 1.20.3+:     Named Binary Tag (NBT). Plain-text components use TAG_String.
//              More complex components use TAG_Compound.
//
// Network NBT wire format for a plain-text component:
//
//   0x08                       — TAG_String type
//   0x00 0x00                  — root name length = 0 (unnamed root tag)
//   hi lo                      — value length (big-endian u16)
//   <bytes>                    — UTF-8 value
//
// Note: Some NBT in Minecraft protocol omits the leading TAG ID and name,
// but for Text Components in many packets (including Login Disconnect),
// the full unnamed tag is expected.
// -----------------------------------------------------------------------------

fn encode_text_component_nbt(text: &str) -> Vec<u8> {
    let mut map = std::collections::HashMap::new();
    map.insert("text".to_string(), fastnbt::Value::String(text.to_string()));
    let val = fastnbt::Value::Compound(map);
    let mut bytes = fastnbt::to_bytes(&val).unwrap();

    tracing::debug!("NBT before stripping name: {:02x?}", bytes);

    // fastnbt::to_bytes produces a named tag: [Type ID] [Name Len (2)] [Name] [Payload]
    // For Value::Compound, it produces 0x0A 0x00 0x00 ...
    // Minecraft network protocol since 1.20.2/1.20.5 often expects an UNNAMED tag.
    // Unnamed TAG_Compound: 0x0A [Tags...] 0x00
    if bytes.len() >= 3 && bytes[0] == 0x0A && bytes[1] == 0x00 && bytes[2] == 0x00 {
        bytes.remove(1);
        bytes.remove(1);
    }
    tracing::debug!("NBT after stripping name: {:02x?}", bytes);
    bytes
}

fn encode_text_component_json(text: &str) -> Vec<u8> {
    // Escape quotes in the text for JSON safety
    let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
    let json = format!(r#"{{"text":"{}"}}"#, escaped);
    tracing::debug!("Encoded JSON: {}", json);
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
    tracing::info!("Building Login Disconnect for protocol version {}: {}", protocol_version, message);
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
    tracing::debug!("Full packet bytes: {:02x?}", packet);
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
/// `HandshakeInfo`; `consume_len` is the number of bytes to discard from the
/// stream (e.g. the peeked Handshake) before writing.
pub async fn send_login_disconnect(
    stream: &mut TcpStream,
    message: &str,
    protocol_version: i32,
    consume_len: usize,
) -> std::io::Result<()> {
    if consume_len > 0 {
        let mut discard = vec![0u8; consume_len];
        let _ = stream.read_exact(&mut discard).await;
    }

    let packet = build_login_disconnect(message, protocol_version);
    // Best-effort: ignore write errors — the important thing is we tried.
    let write_timeout = Duration::from_millis(500);
    let _ = timeout(write_timeout, stream.write_all(&packet)).await;
    let _ = stream.shutdown().await;
    Ok(())
}
