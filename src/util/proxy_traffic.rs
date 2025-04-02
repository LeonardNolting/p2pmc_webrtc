use std::sync::Arc;
use anyhow::Context;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use webrtc::data::data_channel::DataChannel;
use tokio_util::sync::CancellationToken;

pub async fn proxy_traffic(
    data_channel: Arc<DataChannel>,
    tcp_stream: TcpStream,
    cancel_token: CancellationToken
) -> anyhow::Result<()> {
    let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();

    // Create tasks using let bindings to maintain ownership
    let mut read_task: tokio::task::JoinHandle<anyhow::Result<()>> = {
        let data_channel = Arc::clone(&data_channel);
        let cancel_token = cancel_token.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break Ok(()),
                    result = data_channel.read(&mut buf) => {
                        let n = result.context("Failed to read from data channel")?;
                        tcp_write.write_all(&buf[..n]).await
                            .context("Failed to write to TCP stream")?;
                    }
                }
            }
        })
    };

    let mut write_task = {
        let data_channel = Arc::clone(&data_channel);
        let cancel_token = cancel_token.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => break Ok(()),
                    result = tcp_read.read(&mut buf) => {
                        let n = result.context("Failed to read from TCP stream")?;
                        if n == 0 { // EOF
                            return Ok(());
                        }
                        data_channel.write(&Bytes::copy_from_slice(&buf[..n])).await
                            .context("Failed to write to data channel")?;
                    }
                }
            }
        })
    };

    // Use mutable references to avoid moving tasks
    let result = tokio::select! {
        res = &mut read_task => res,
        res = &mut write_task => res,
    };

    // Trigger cancellation
    cancel_token.cancel();

    // Now properly await both using their original bindings
    let (read_res, write_res) = tokio::join!(read_task, write_task);

    // Handle results with proper error propagation
    result.context("Primary proxy error")??;
    read_res.context("Read task error")??;
    write_res.context("Write task error")??;

    Ok(())
}