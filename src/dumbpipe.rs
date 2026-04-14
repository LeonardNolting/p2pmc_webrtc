use flutter_rust_bridge::frb;
use iroh::{
    endpoint::{presets, Accepting},
    Endpoint, EndpointAddr, SecretKey,
};
use iroh_tickets::endpoint::EndpointTicket;
use n0_error::{bail_any, ensure_any, AnyError, StdResultExt};
use std::{
    io,
    net::{SocketAddr, SocketAddrV4, ToSocketAddrs},
    str::FromStr,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    select,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use webrtc::util::Conn;

const ALPN: &[u8] = b"DUMBPIPEV0";
const HANDSHAKE: [u8; 5] = *b"hello";
const ONLINE_TIMEOUT: Duration = Duration::from_secs(5);

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
            send.reset(0u8.into()).ok();
            Err(io::Error::other("cancelled"))
        }
    }
}

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

fn get_or_create_secret() -> n0_error::Result<SecretKey> {
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

async fn create_endpoint(
    secret_key: SecretKey,
    ipv4_addr: Option<SocketAddrV4>,
    alpns: Vec<Vec<u8>>,
) -> n0_error::Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(alpns);
    if let Some(addr) = ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
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
/// reader/writer pair. Ties to the global token via a child token.
async fn forward_bidi(
    global_token: CancellationToken,
    from1: impl AsyncRead + Send + Sync + Unpin + 'static,
    to1: impl AsyncWrite + Send + Sync + Unpin + 'static,
    from2: noq::RecvStream,
    to2: noq::SendStream,
) -> n0_error::Result<()> {
    // Create a child token. If the global_token is cancelled, this cancels.
    // If we cancel this manually, it does NOT cancel the global_token.
    let local_token = global_token.child_token();
    let token_for_stdout = local_token.clone();

    let forward_from_stdin = tokio::spawn(async move {
        copy_to_noq(from1, to2, local_token.clone())
            .await
            .map_err(cancel_token(local_token))
    });
    let forward_to_stdout = tokio::spawn(async move {
        copy_from_noq(from2, to1, token_for_stdout.clone())
            .await
            .map_err(cancel_token(token_for_stdout))
    });

    forward_to_stdout.await.anyerr()?.anyerr()?;
    forward_from_stdin.await.anyerr()?.anyerr()?;
    Ok(())
}

/// Listen on a tcp port and forward incoming connections to an endpoint.
pub async fn connect_tcp(
    token: CancellationToken,
    ipv4_addr: Option<String>,
    addr: String,
    ticket: String,
) -> n0_error::Result<()> {
    let ticket = EndpointTicket::from_str(&ticket).std_context("invalid ticket")?;
    let ipv4_addr = ipv4_addr.map(|s| s.parse::<SocketAddrV4>().expect("invalid ipv4 address"));
    let addrs = addr
        .to_socket_addrs()
        .std_context(format!("invalid host string {}", addr))?;
    let secret_key = get_or_create_secret()?;
    let endpoint = create_endpoint(secret_key, ipv4_addr, vec![])
        .await
        .std_context("unable to bind endpoint")?;
    tracing::info!("tcp listening on {:?}", addrs);

    if (timeout(ONLINE_TIMEOUT, endpoint.online()).await).is_err() {
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
        token: CancellationToken,
        next: io::Result<(tokio::net::TcpStream, SocketAddr)>,
        addr: EndpointAddr,
        endpoint: Endpoint,
        handshake: bool,
        alpn: &[u8],
    ) -> n0_error::Result<()> {
        let (tcp_stream, tcp_addr) = next.std_context("error accepting tcp connection")?;
        let (tcp_recv, tcp_send) = tcp_stream.into_split();
        tracing::info!("got tcp connection from {}", tcp_addr);
        let remote_endpoint_id = addr.id;
        let connection = endpoint
            .connect(addr, alpn)
            .await
            .std_context(format!("error connecting to {remote_endpoint_id}"))?;
        let (mut endpoint_send, endpoint_recv) = connection
            .open_bi()
            .await
            .std_context(format!("error opening bidi stream to {remote_endpoint_id}"))?;

        if handshake {
            endpoint_send.write_all(&HANDSHAKE).await.anyerr()?;
        }
        // Pass the token down to the stream handler
        forward_bidi(token, tcp_recv, tcp_send, endpoint_recv, endpoint_send).await?;
        Ok::<_, AnyError>(())
    }

    let addr = ticket.endpoint_addr();
    loop {
        // Wait for connection or external cancellation
        let next = tokio::select! {
            stream = tcp_listener.accept() => stream,
            _ = token.cancelled() => {
                tracing::info!("cancellation requested, stopping tcp listener");
                break;
            }
        };
        let endpoint = endpoint.clone();
        let addr = addr.clone();
        let handshake = !false;
        let alpn = ALPN.to_vec();
        let token_clone = token.clone();

        tokio::spawn(async move {
            if let Err(cause) =
                handle_tcp_accept(token_clone, next, addr, endpoint, handshake, &alpn).await
            {
                tracing::warn!("error handling connection: {}", cause);
            }
        });
    }
    Ok(())
}

#[frb(opaque)]
pub struct ConnectionManager {
    endpoint: Endpoint,
    addrs: Vec<SocketAddr>,
    ticket: EndpointTicket,
    host: String,
}

pub async fn create_connection_manager(
    host: String,
    ipv4_addr: Option<String>,
) -> n0_error::Result<ConnectionManager> {
    let ipv4_addr = ipv4_addr.map(|s| s.parse::<SocketAddrV4>().expect("invalid ipv4 address"));
    let addrs = match host.to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(e) => bail_any!("invalid host string {}: {}", host, e),
    };
    let secret_key = get_or_create_secret()?;
    let endpoint = create_endpoint(secret_key, ipv4_addr, vec![ALPN.to_vec()]).await?;

    if (timeout(ONLINE_TIMEOUT, endpoint.online()).await).is_err() {
        eprintln!("Warning: Failed to connect to the home relay");
    }
    let addr = endpoint.addr();
    let ticket = EndpointTicket::new(addr.clone());

    eprintln!("Forwarding incoming requests to '{}'.", host);
    eprintln!("To connect, use e.g.:");
    eprintln!("dumbpipe connect-tcp {ticket}");

    tracing::info!("endpoint id is {}", ticket.endpoint_addr().id);
    tracing::info!(
        "relay url is {:?}",
        ticket
            .endpoint_addr()
            .relay_urls()
            .next()
            .map_or("None".to_string(), |url| url.to_string())
    );

    Ok(ConnectionManager {
        endpoint,
        addrs,
        host,
        ticket,
    })
}

/// Listen on an endpoint and forward incoming connections to a tcp socket.
pub async fn listen_tcp(token: CancellationToken, connection_manager: ConnectionManager) {
    // 2. Clone variables needed for the background task
    let loop_endpoint = connection_manager.endpoint.clone();
    let loop_token = token.clone();
    let loop_addrs = connection_manager.addrs.clone();
    let loop_host = connection_manager.host.clone();

    async fn handle_endpoint_accept(
        token: CancellationToken,
        accepting: Accepting,
        addrs: Vec<std::net::SocketAddr>,
        handshake: bool,
    ) -> n0_error::Result<()> {
        let connection = accepting.await.std_context("error accepting connection")?;
        let remote_endpoint_id = &connection.remote_id();
        tracing::info!("got connection from {}", remote_endpoint_id);
        let (s, mut r) = connection
            .accept_bi()
            .await
            .std_context("error accepting stream")?;
        tracing::info!("accepted bidi stream from {}", remote_endpoint_id);
        if handshake {
            let mut buf = [0u8; HANDSHAKE.len()];
            r.read_exact(&mut buf).await.anyerr()?;
            ensure_any!(buf == HANDSHAKE, "invalid handshake");
        }
        let connection = tokio::net::TcpStream::connect(addrs.as_slice())
            .await
            .std_context(format!("error connecting to {addrs:?}"))?;
        let (read, write) = connection.into_split();
        // Pass the token down to the stream handler
        forward_bidi(token, read, write, r, s).await?;
        Ok(())
    }

    eprintln!("Forwarding incoming requests to '{}'.", loop_host);

    loop {
        let incoming = tokio::select! {
            incoming = loop_endpoint.accept() => incoming,
            _ = loop_token.cancelled() => {
                tracing::info!("cancellation requested, stopping endpoint listener");
                break;
            }
        };

        let Some(incoming) = incoming else {
            break;
        };
        let Ok(connecting) = incoming.accept() else {
            break;
        };

        let inner_addrs = loop_addrs.clone();
        let inner_token = loop_token.clone();

        // Re-use your existing logic for handling individual streams
        tokio::spawn(async move {
            if let Err(cause) =
                handle_endpoint_accept(inner_token, connecting, inner_addrs, true).await
            {
                tracing::warn!("error handling connection: {}", cause);
            }
        });
    }
}

#[frb(sync)]
pub fn connection_manager_get_ticket_string(connection_manager: &ConnectionManager) -> String {
    connection_manager.ticket.to_string()
}

fn create_short_ticket(addr: &EndpointAddr) -> EndpointTicket {
    let mut short = EndpointAddr::new(addr.id);
    for relay_url in addr.relay_urls() {
        short = short.with_relay_url(relay_url.clone());
    }
    short.into()
}
