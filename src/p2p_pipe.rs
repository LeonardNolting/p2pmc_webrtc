use std::{
    io,
    net::{SocketAddr, SocketAddrV6, ToSocketAddrs},
    str::FromStr,
    time::Duration,
};
use std::sync::Arc;
use anyhow::Context;
use clap::{Parser, Subcommand};
use iroh::{
    endpoint::{presets, Accepting},
    Endpoint, EndpointAddr, SecretKey,
};
use iroh_tickets::endpoint::EndpointTicket;
use n0_error::{bail_any, ensure_any, AnyError, Result, StdResultExt};
use pkarr::Client;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    select,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
#[cfg(unix)]
use {
    std::path::PathBuf,
    tokio::net::{UnixListener, UnixStream},
};
use crate::dht::{lookup_iroh_mapping, publish_iroh_mapping};
use crate::util::parse_server::{parse_handshake};
use crate::util::mc_disconnect::{send_login_disconnect};

pub use std::net::SocketAddrV4;

/// The ALPN for dumbpipe.
///
/// It is basically just passing data through 1:1, except that the connecting
/// side will send a fixed size handshake to make sure the stream is created.
const ALPN: &[u8] = b"DUMBPIPEV0";

/// The handshake to send when connecting.
///
/// The side that calls open_bi() first must send this handshake, the side that
/// calls accept_bi() must consume it.
const HANDSHAKE: [u8; 5] = *b"hello";

const ONLINE_TIMEOUT: Duration = Duration::from_secs(5);

/// Copy from a reader to a noq stream.
///
/// Will send a reset to the other side if the operation is cancelled, and fail
/// with an error.
///
/// Returns the number of bytes copied in case of success.
async fn copy_to_noq(
    mut from: impl AsyncRead + Unpin,
    mut send: noq::SendStream,
    token: CancellationToken,
) -> io::Result<u64> {
    tracing::trace!("copying to noq");
    tokio::select! {
        res = tokio::io::copy(&mut from, &mut send) => {
            let size = res?;
            send.finish()?;
            Ok(size)
        }
        _ = token.cancelled() => {
            // send a reset to the other side immediately
            send.reset(0u8.into()).ok();
            Err(io::Error::other("cancelled"))
        }
    }
}

/// Copy from a noq stream to a writer.
///
/// Will send stop to the other side if the operation is cancelled, and fail
/// with an error.
///
/// Returns the number of bytes copied in case of success.
async fn copy_from_noq(
    mut recv: noq::RecvStream,
    mut to: impl AsyncWrite + Unpin,
    token: CancellationToken,
) -> io::Result<u64> {
    tokio::select! {
        res = tokio::io::copy(&mut recv, &mut to) => {
            Ok(res?)
        },
        _ = token.cancelled() => {
            recv.stop(0u8.into()).ok();
            Err(io::Error::other("cancelled"))
        }
    }
}

/// Get the secret key or generate a new one.
///
/// Print the secret key to stderr if it was generated, so the user can save it.
fn get_or_create_secret() -> Result<SecretKey> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => SecretKey::from_str(&secret).std_context("invalid secret"),
        Err(_) => {
            let key = SecretKey::generate(&mut rand::rng());
            eprintln!(
                "using secret key {}",
                data_encoding::HEXLOWER.encode(&key.to_bytes())
            );
            Ok(key)
        }
    }
}

/// Create a new iroh endpoint.
async fn create_endpoint(
    secret_key: SecretKey,
    ipv4_addr: Option<SocketAddrV4>,
    alpns: Vec<Vec<u8>>,
) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(alpns);
    if let Some(addr) = ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    /*if let Some(addr) = common.ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }*/
    let endpoint = builder.bind().await.anyerr()?;
    Ok(endpoint)
}

fn cancel_token<T>(token: CancellationToken) -> impl Fn(T) -> T {
    move |x| {
        token.cancel();
        x
    }
}

/// Bidirectionally forward data from a noq stream and an arbitrary tokio
/// reader/writer pair, aborting both sides when either one forwarder is done,
/// or when control-c is pressed.
async fn forward_bidi(
    from1: impl AsyncRead + Send + Sync + Unpin + 'static,
    to1: impl AsyncWrite + Send + Sync + Unpin + 'static,
    from2: noq::RecvStream,
    to2: noq::SendStream,
) -> Result<()> {
    let token1 = CancellationToken::new();
    let token2 = token1.clone();
    let token3 = token1.clone();
    let forward_from_stdin = tokio::spawn(async move {
        copy_to_noq(from1, to2, token1.clone())
            .await
            .map_err(cancel_token(token1))
    });
    let forward_to_stdout = tokio::spawn(async move {
        copy_from_noq(from2, to1, token2.clone())
            .await
            .map_err(cancel_token(token2))
    });
    let _control_c = tokio::spawn(async move {
        tokio::signal::ctrl_c().await?;
        token3.cancel();
        io::Result::Ok(())
    });
    forward_to_stdout.await.anyerr()?.anyerr()?;
    forward_from_stdin.await.anyerr()?.anyerr()?;
    Ok(())
}

pub async fn p2p_client(
    pkarr_client: Client,
    addr: String,
    ipv4_addr: Option<SocketAddrV4>,
    cancel_token: CancellationToken, // 1. Inject the token from Flutter
) -> Result<()> {
    let pkarr_client = Arc::new(pkarr_client);

    let addrs = addr
        .to_socket_addrs()
        .std_context(format!("invalid host string {}", addr))?;
    let secret_key = get_or_create_secret()?;
    let endpoint = create_endpoint(secret_key, ipv4_addr, vec![])
        .await
        .std_context("unable to bind endpoint")?;
    tracing::info!("tcp listening on {:?}", addrs);

    // Wait for our own endpoint to be ready before trying to connect.
    if (tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await).is_err() {
        eprintln!("Warning: Failed to connect to the home relay");
    }

    let tcp_listener = match tokio::net::TcpListener::bind(addrs.as_slice()).await {
        Ok(tcp_listener) => tcp_listener,
        Err(cause) => {
            tracing::error!("error binding tcp socket to {:?}: {}", addrs, cause);
            return Ok(());
        }
    };

    async fn handle_tcp_accept(
        pkarr_client: Arc<Client>,
        next: io::Result<(tokio::net::TcpStream, SocketAddr)>,
        endpoint: Endpoint,
        handshake: bool,
        alpn: &[u8],
    ) -> Result<()> {
        let (mut tcp_stream, tcp_addr) = match next {
            Ok(v) => v,
            Err(e) => return Err(e.into()),
        };

        tracing::info!("got tcp connection from {}", tcp_addr);

        // ---- parse handshake ----
        // We now get both the server ID *and* the protocol version (needed to send
        // correctly-encoded Login Disconnect packets).
        let handshake_info = match parse_handshake(&mut tcp_stream).await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("error parsing handshake: {e}");
                // Can't send Login Disconnect here — we don't even know if this is
                // a Login connection or a Status ping.  Bare close is correct.
                let _ = tcp_stream.shutdown().await;
                return Ok(());
            }
        };

        // Helper: send a Login Disconnect then return Ok(()).
        // Only meaningful when next_state == 2 (Login); for Status pings (1) we
        // just close — the client is doing a server list refresh, not joining.
        macro_rules! disconnect {
        ($msg:expr) => {{
            if handshake_info.next_state == 2 {
                let _ = send_login_disconnect(
                    &mut tcp_stream,
                    $msg,
                    handshake_info.protocol_version,
                )
                .await;
            } else {
                let _ = tcp_stream.shutdown().await;
            }
            return Ok(());
        }};
    }

        let server = format!("{}.jude.gg", handshake_info.server_id);

        // ---- lookup ticket ----
        let ticket = match lookup_iroh_mapping(pkarr_client, server.clone()).await {
            Ok(Some(ticket)) => ticket,
            Ok(None) => {
                tracing::warn!("ticket not found for {server}");
                disconnect!(&format!(
                "Server '{}' not found. Check the address and try again.",
                handshake_info.server_id
            ));
            }
            Err(e) => {
                tracing::error!("ticket lookup failed for {server}: {e}");
                // This is an internal error on our side, not a user mistake.
                disconnect!("Proxy error: failed to look up server. Please try again later.");
            }
        };

        // ---- parse ticket ----
        let ticket = match EndpointTicket::from_str(&ticket) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("invalid ticket for {server}: {e}");
                disconnect!(&format!(
                "Server '{}' has an invalid configuration. Contact the server owner.",
                handshake_info.server_id
            ));
            }
        };

        let addr = ticket.endpoint_addr().to_owned();
        let remote_endpoint_id = addr.id;

        // ---- connect ----
        let connection = match endpoint.connect(addr, alpn).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("connect failed to {remote_endpoint_id}: {e}");
                disconnect!(&format!(
                "Could not reach server '{}'. It may be offline.",
                handshake_info.server_id
            ));
            }
        };

        // ---- open bidi stream ----
        let (mut endpoint_send, endpoint_recv) = match connection.open_bi().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("open_bi failed to {remote_endpoint_id}: {e}");
                disconnect!(&format!(
                "Connected to '{}' but could not open a stream. Try again.",
                handshake_info.server_id
            ));
            }
        };

        // ---- handshake ----
        if handshake {
            if let Err(e) = endpoint_send.write_all(&HANDSHAKE).await {
                tracing::warn!("iroh handshake failed: {e}");
                disconnect!("Proxy handshake failed. Please try again.");
            }
        }

        // ---- only split once everything is ready ----
        let (tcp_recv, tcp_send) = tcp_stream.into_split();

        // ---- forwarding ----
        if let Err(e) = forward_bidi(tcp_recv, tcp_send, endpoint_recv, endpoint_send).await {
            tracing::warn!("forwarding failed: {e}");
            // Stream halves drop here; connection closes naturally.
            return Ok(());
        }

        Ok(())
    }
    // 2. Hand ownership of the infinite loop over to Tokio.
    tokio::spawn(async move {
        loop {
            let alpn = ALPN.to_vec();
            let handshake = !false;

            let endpoint = endpoint.clone();
            let pkarr_client = pkarr_client.clone();

            // 3. tokio::select! listens for either a new TCP connection or the cancel signal
            let next = tokio::select! {
                stream = tcp_listener.accept() => stream,
                _ = cancel_token.cancelled() => {
                    tracing::info!("Cancellation requested via token. Shutting down P2P client loop.");
                    let _ = &endpoint.close().await;
                    break;
                }
            };

            // Spawn the connection handler so the main loop isn't blocked
            tokio::spawn(async move {
                if let Err(cause) = handle_tcp_accept(pkarr_client, next, endpoint, handshake, &alpn).await {
                    tracing::warn!("error handling connection: {}", cause);
                }
            });
        }
    });

    // 4. Return successfully to Flutter right after the task is spawned.
    Ok(())
}

/// Listen on an endpoint and forward incoming connections to a tcp socket.
pub async fn p2p_server(
    host: String,
    url_name: String,
    ipv4_addr: Option<SocketAddrV4>,
    cancel_token: CancellationToken, // Inject the token from Flutter here
) -> Result<()> {
    let addrs = match host.to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(e) => bail_any!("invalid host string {}: {}", host, e),
    };
    let secret_key = get_or_create_secret()?;
    // let secret_key = SecretKey::from_str("7e7401dc4939595037d7cd24e24b827b5f6794aa6910b9eb9280425416e1eec8").std_context("invalid secret")?;
    let endpoint = create_endpoint(secret_key, ipv4_addr, vec![ALPN.into()]).await?;
    // wait for the endpoint to figure out its address before making a ticket
    if (tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await).is_err() {
        eprintln!("Warning: Failed to connect to the home relay");
    }
    let addr = endpoint.addr();
    let ticket = EndpointTicket::new(addr);

    // print the ticket on stderr so it doesn't interfere with the data itself
    //
    // note that the tests rely on the ticket being the last thing printed
    eprintln!("Forwarding incoming requests to '{}'.", host);
    eprintln!("To connect, use e.g.:");
    eprintln!("dumbpipe connect-tcp {ticket}");
    /*if args.common.verbose > 0 {
        eprintln!("or:\ndumbpipe connect-tcp {short}");
    }*/
    tracing::info!("endpoint id is {}", ticket.endpoint_addr().id);
    tracing::info!(
        "relay url is {:?}",
        ticket
            .endpoint_addr()
            .relay_urls()
            .next()
            .map_or("None".to_string(), |url| url.to_string())
    );

    publish_iroh_mapping(
        Arc::new(Client::builder().build().unwrap()), // TODO use client with min/max ttl?
        url_name,
        ticket.to_string(),
        CancellationToken::new(),
        None,
        None,
    ).await?;

    // We move handle_endpoint_accept inside so it can be easily
    // captured or used by the spawned task without lifetime issues.
    async fn handle_endpoint_accept(
        accepting: Accepting,
        addrs: Vec<std::net::SocketAddr>,
        handshake: bool,
    ) -> Result<()> {
        let connection = accepting.await.std_context("error accepting connection")?;
        let remote_endpoint_id = &connection.remote_id();
        tracing::info!("got connection from {}", remote_endpoint_id);
        let (s, mut r) = connection
            .accept_bi()
            .await
            .std_context("error accepting stream")?;
        tracing::info!("accepted bidi stream from {}", remote_endpoint_id);
        if handshake {
            // read the handshake and verify it
            let mut buf = [0u8; HANDSHAKE.len()];
            r.read_exact(&mut buf).await.anyerr()?;
            ensure_any!(buf == HANDSHAKE, "invalid handshake");
        }
        let connection = tokio::net::TcpStream::connect(addrs.as_slice())
            .await
            .std_context(format!("error connecting to {addrs:?}"))?;
        let (read, write) = connection.into_split();
        forward_bidi(read, write, r, s).await?;
        Ok(())
    }

    // Hand ownership of the infinite loop over to Tokio.
    // The FFI future will resolve successfully right after this block.
    tokio::spawn(async move {
        loop {
            let incoming = select! {
                incoming = endpoint.accept() => incoming,
                _ = cancel_token.cancelled() => {
                    // Replaces the ctrl_c system signal with our application-level signal
                    tracing::info!("Cancellation requested via token. Shutting down P2P server loop.");
                    let _ = &endpoint.close().await;
                    break;
                }
            };

            let Some(incoming) = incoming else {
                break;
            };
            let Ok(connecting) = incoming.accept() else {
                break;
            };

            let addrs = addrs.clone();
            let handshake = !false;

            tokio::spawn(async move {
                if let Err(cause) = handle_endpoint_accept(connecting, addrs, handshake).await {
                    tracing::warn!("error handling connection: {}", cause);
                }
            });
        }
    });

    // Return successfully to Flutter. The server is now running autonomously.
    Ok(())
}