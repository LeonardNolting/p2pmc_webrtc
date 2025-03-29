use std::io::Result;

use tokio::net::TcpStream;
use tracing::info;
use url::Url;

pub(crate) async fn parse_server(stream: &mut TcpStream) -> anyhow::Result<String> {
    let server_address = get_server_address(stream).await?;
    let domain = Url::parse(&server_address).map_or_else(|error| {
        server_address
    }, move |url| {
        let domain = url.domain().expect("The client connected not via a domain, can't read domain").to_string();
        domain
    });
    let mut domains: Vec<&str> = domain.split(".").collect();
    domains.reverse();
     if domains.len() != 3 ||  domains[0] != "gg" || domains[1] != "jude" {
        // TODO is panic correct here?
        panic!("Couldn't read subdomain from URL, domains: {:?}", domains);
    } 
    let to_id = domains.last().unwrap().to_string();
    info!("Parsed server from domain: {to_id}");
    Ok(to_id)
}

pub(crate) async fn get_server_address(stream: &mut TcpStream) -> Result<String> {
    // Create a buffer large enough for the initial handshake packet
    // TODO let length depend on size of first packet
    let mut peek_buf = vec![0u8; 1024];

    // Peek at the data without consuming it
    let bytes_read = stream.peek(&mut peek_buf).await?;

    if bytes_read == 0 {
        // TODO handle properly .. after leaving?
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Connection closed before receiving handshake"
        ));
    }

    // Create a cursor to read the peeked data
    let mut reader = std::io::Cursor::new(&peek_buf[..bytes_read]);

    // Read packet length (VarInt)
    let mut packet_length = 0;
    let mut shift = 0;
    loop {
        let byte = reader.get_ref()[reader.position() as usize];
        reader.set_position(reader.position() + 1);

        packet_length |= ((byte & 0b0111_1111) as i32) << shift;
        if (byte & 0b1000_0000) == 0 {
            break;
        }
        shift += 7;
    }

    // Read packet ID (VarInt) - we don't need this but must skip it
    let mut shift = 0;
    loop {
        let byte = reader.get_ref()[reader.position() as usize];
        reader.set_position(reader.position() + 1);

        if (byte & 0b1000_0000) == 0 {
            break;
        }
        shift += 7;
    }

    // Read protocol version (VarInt) - skip it
    let mut shift = 0;
    loop {
        let byte = reader.get_ref()[reader.position() as usize];
        reader.set_position(reader.position() + 1);

        if (byte & 0b1000_0000) == 0 {
            break;
        }
        shift += 7;
    }

    // Read server address length (VarInt)
    let mut address_length = 0;
    let mut shift = 0;
    loop {
        let byte = reader.get_ref()[reader.position() as usize];
        reader.set_position(reader.position() + 1);

        address_length |= ((byte & 0b0111_1111) as i32) << shift;
        if (byte & 0b1000_0000) == 0 {
            break;
        }
        shift += 7;
    }

    // Read the server address
    let start_pos = reader.position() as usize;
    let address_bytes = &reader.get_ref()[start_pos..start_pos + address_length as usize];
    let server_address = String::from_utf8_lossy(address_bytes).to_string();

    Ok(server_address)
}