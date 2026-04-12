use std::io;
use std::net::{SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::str::FromStr;
use crate::core::p2p::offer_reply::Offer;
use crate::core::p2p::peer::PeerId;
use crate::core::p2p::peer_connector::{PeerConnectionCreator, PeerListenerCreator};
use crate::core::p2p::session::Session;
use crate::util::minecraft_connector::MinecraftConnector;
use crate::util::proxy_traffic::proxy_traffic;
use cancellable::cancellable;
use std::sync::Arc;
use std::time::Duration;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use iroh::endpoint::{presets, Accepting, ToSocketAddr};
use iroh_tickets::endpoint::EndpointTicket;
use n0_error::{bail_any, ensure_any, AnyError, StdResultExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

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

/// Create a new iroh endpoint.
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
    // if let Some(addr) = common.ipv6_addr {
    //     builder = builder.bind_addr(addr)?;
    // }
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
) -> n0_error::Result<()> {
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

/// Listen on a tcp port and forward incoming connections to an endpoint.
pub async fn connect_tcp(ipv4_addr: Option<String>, addr: String, ticket: String) -> n0_error::Result<()> {
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

    // Wait for our own endpoint to be ready before trying to connect.
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
        // send the handshake unless we are using a custom alpn
        // when using a custom alpn, everything is up to the user
        if handshake {
            // the connecting side must write first. we don't know if there will be something
            // on stdin, so just write a handshake.
            endpoint_send
                .write_all(&HANDSHAKE)
                .await
                .anyerr()?;
        }
        forward_bidi(tcp_recv, tcp_send, endpoint_recv, endpoint_send).await?;
        Ok::<_, AnyError>(())
    }
    let addr = ticket.endpoint_addr();
    loop {
        // also wait for ctrl-c here so we can use it before accepting a connection
        let next = tokio::select! {
            stream = tcp_listener.accept() => stream,
            _ = tokio::signal::ctrl_c() => {
                eprintln!("got ctrl-c, exiting");
                break;
            }
        };
        let endpoint = endpoint.clone();
        let addr = addr.clone();
        let handshake = !false;
        let alpn = ALPN.to_vec();
        tokio::spawn(async move {
            if let Err(cause) = handle_tcp_accept(next, addr, endpoint, handshake, &alpn).await {
                // log error at warn level
                //
                // we should know about it, but it's not fatal
                tracing::warn!("error handling connection: {}", cause);
            }
        });
    }
    Ok(())
}

/// Listen on an endpoint and forward incoming connections to a tcp socket.
pub async fn listen_tcp(ipv4_addr: Option<String>, host: String) -> n0_error::Result<()> {
    let ipv4_addr = ipv4_addr.map(|s| s.parse::<SocketAddrV4>().expect("invalid ipv4 address"));
    let addrs = match host.to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(e) => bail_any!("invalid host string {}: {}", host, e),
    };
    let secret_key = get_or_create_secret()?;
    let endpoint = create_endpoint(secret_key, ipv4_addr, vec![ALPN.to_vec()]).await?;
    // wait for the endpoint to figure out its address before making a ticket
    if (timeout(ONLINE_TIMEOUT, endpoint.online()).await).is_err() {
        eprintln!("Warning: Failed to connect to the home relay");
    }
    let addr = endpoint.addr();
    let short = create_short_ticket(&addr);
    let ticket = EndpointTicket::new(addr);

    // print the ticket on stderr so it doesn't interfere with the data itself
    //
    // note that the tests rely on the ticket being the last thing printed
    eprintln!("Forwarding incoming requests to '{}'.", host);
    eprintln!("To connect, use e.g.:");
    eprintln!("dumbpipe connect-tcp {ticket}");
    // if args.common.verbose > 0 {
    //     eprintln!("or:\ndumbpipe connect-tcp {short}");
    // }
    tracing::info!("endpoint id is {}", ticket.endpoint_addr().id);
    tracing::info!(
        "relay url is {:?}",
        ticket
            .endpoint_addr()
            .relay_urls()
            .next()
            .map_or("None".to_string(), |url| url.to_string())
    );

    // handle a new incoming connection on the endpoint
    async fn handle_endpoint_accept(
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

    loop {
        let incoming = select! {
            incoming = endpoint.accept() => incoming,
            _ = tokio::signal::ctrl_c() => {
                eprintln!("got ctrl-c, exiting");
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
                // log error at warn level
                //
                // we should know about it, but it's not fatal
                tracing::warn!("error handling connection: {}", cause);
            }
        });
    }
    Ok(())
}

/// Creates a ticket that only includes the id and any relay urls
fn create_short_ticket(addr: &EndpointAddr) -> EndpointTicket {
    let mut short = EndpointAddr::new(addr.id);
    for relay_url in addr.relay_urls() {
        short = short.with_relay_url(relay_url.clone());
    }
    short.into()
}