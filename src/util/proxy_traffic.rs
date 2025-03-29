use std::sync::Arc;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn proxy_traffic(
    data_channel: Arc<webrtc::data::data_channel::DataChannel>,
    tcp_stream: TcpStream,
) -> anyhow::Result<()> {
    let (mut tcp_read, mut tcp_write) = tokio::io::split(tcp_stream);
    let data_channel_clone = data_channel.clone();

    let read_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match data_channel_clone.read(&mut buf).await {
                Ok(n) => {
                    if let Err(e) = tcp_write.write_all(&buf[..n]).await {
                        eprintln!("TCP write error: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Data channel read error: {}", e);
                    break;
                }
            }
        }
    });

    let write_task = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if let Err(e) = data_channel.write(&Bytes::copy_from_slice(&buf[..n])).await {
                        eprintln!("Data channel write error: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("TCP read error: {}", e);
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = read_task => {},
        _ = write_task => {},
    }

    Ok(())
}