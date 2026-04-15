pub use crate::core::p2p::session;
use flutter_rust_bridge::frb;
use std::sync::Arc;
use std::thread::JoinHandle;
// pub use crate::core::proxies::client::jude_client;
// pub use crate::core::proxies::server::jude_server;
pub use tokio_util::sync::CancellationToken;

pub mod core;
pub mod crypto;
pub mod util;
pub mod nbt;
pub mod dumbpipe;
pub mod dht;
pub mod p2p_pipe;

pub use pkarr::Client;

pub use tokio::task::AbortHandle;

#[frb(external)]
impl AbortHandle {
    pub fn abort(&self) {}
}

async fn test3() -> String {
    "test3".to_string()
}
async fn test2() -> Option<String> {
    let cloned_token = CancellationToken::new();

    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            // Wait for either cancellation or a very long time
            tokio::select! {
                        _ = cloned_token.cancelled() => None,
                        value = test3() => Some(value)
                    }
        })
    });
    return handle.join().unwrap();
}

/*#[frb(sync)]
pub fn new_cancellation_token() -> Arc<CancellationToken> {
    Arc::new(CancellationToken::new())
}

#[frb(sync)]
pub fn cancel_cancellation_token(token: Arc<CancellationToken>) {
    token.cancel();
}*/

#[tokio::main(flavor = "current_thread")]
async fn test() {}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use crate::dumbpipe::listen_tcp;
    use crate::session::Session;
    use crate::util::logging::start_logger;
    use crate::util::crypto;
    use crate::util::run_minecraft_vanilla_server::run_minecraft_vanilla_server_cancellable;

    #[tokio::test]
    async fn test_listen_tcp() {
        // listen_tcp(CancellationToken::new(), None, "localhost:5200".to_string()).await.unwrap();
    }
}
