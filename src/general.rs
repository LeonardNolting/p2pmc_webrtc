use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use rust_socketio::client::Client;
use rust_socketio::{ClientBuilder, Payload, RawClient};
use serde::{Deserialize, Serialize};
use tokio::task;
use webrtc::data_channel::OnCloseHdlrFn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OfferReply {
    pub id: String,
    pub to: String,
    pub number: u32,
    pub description: String,
}

pub type OfferReplyHdlrFn = Box<
    dyn (FnMut(OfferReply, RawClient) -> Pin<Box<dyn Future<Output=()> + Send + 'static>>)
    + Send
    + Sync,
>;

pub fn test(c: OnCloseHdlrFn) {}

// async fn connect_to_signaling_server<Fut: Future, F: (Fn(OfferReply, RawClient) -> Fut) + Send + 'static>(
pub(crate) async fn connect_to_signaling_server<OfferHandler, OfferHandlerFuture, ReplyHandler, ReplyHandlerFuture>(
    host: &str,
    on_offer: OfferHandler,
    on_reply: ReplyHandler,
) -> Client
where
    OfferHandler: Fn(OfferReply, RawClient) -> OfferHandlerFuture + Send + Sync + 'static,
    OfferHandlerFuture: Future<Output=()> + Send + 'static,
    ReplyHandler: Fn(OfferReply, RawClient) -> ReplyHandlerFuture + Send + Sync + 'static,
    ReplyHandlerFuture: Future<Output=()> + Send + 'static,
{
    let host = host.to_owned();
    let on_reply = Arc::new(on_reply);
    let on_offer = Arc::new(on_offer);
    let (open_tx, mut open_rx) = tokio::sync::mpsc::channel::<()>(1);
    let socket = task::spawn_blocking({
        let host = host.clone();
        move || {
            println!("Connecting to signaling server {host}");

            let mut builder = ClientBuilder::new(host.clone());

            let rt_handle = tokio::runtime::Handle::current();
            let rt_handle_1 = rt_handle.clone();
            let rt_handle_2 = rt_handle.clone();
            let rt_handle_3 = rt_handle.clone();
            let open_tx = open_tx.clone();

            builder = builder
                .on(
                    "connections:offer",
                    move |payload: Payload, socket: RawClient| {
                        let Payload::Text(json) = payload else {
                            unreachable!()
                        };
                        let offer: OfferReply =
                            serde_json::from_value(json.first().unwrap().clone()).unwrap();
                        let on_offer = Arc::clone(&on_offer);
                        rt_handle_1.spawn(async move {
                            on_offer(offer, socket).await;
                        });
                    },
                )
                .on(
                    "connections:reply",
                    move |payload: Payload, socket: RawClient| {
                        let Payload::Text(json) = payload else {
                            unreachable!()
                        };
                        let reply: OfferReply =
                            serde_json::from_value(json.first().unwrap().clone()).unwrap();
                        let on_reply = Arc::clone(&on_reply);
                        rt_handle_2.spawn(async move {
                            on_reply(reply, socket).await;
                        });
                    },
                )
                .on("open", move |payload, socket| {
                    let open_tx = open_tx.clone();
                    rt_handle_3.spawn(async move {
                        open_tx.send(()).await.unwrap();
                    });
                })
                .on("error", |err, _| eprintln!("Error: {:#?}", err))
                .on("close", |_, socket: RawClient| println!("Disconnected"));

            let socket = builder.connect().expect("Connection failed");

            socket
        }
    }).await.expect("spawn_blocking panicked");

    open_rx.recv().await.unwrap();

    println!("Connected to signaling server {host}");
    
    socket

    // TODO disconnect when app closes
    // TODO does it already automatically disconnect when app closes? check with signaling server by listening for disconnects and closing the app!
    // socket.disconnect().expect("Disconnect failed")
}

pub(crate) async fn register(id: &str, socket: &Client) {
    let id = id.to_owned();
    let socket = socket.clone();
    task::spawn_blocking(move || {
        socket
            .emit_with_ack(
                "connections:register",
                id.clone(),
                Duration::from_secs(2),
                |message: Payload, _| {
                    println!("connections:register was acked: {:#?}", message);
                },
            )
            .expect("Server unreachable");

        println!("Registered on signaling server as {id}");
    }).await.unwrap();
}