pub use crate::core::p2p::session;
use flutter_rust_bridge::frb;
use std::sync::Arc;
// pub use crate::core::proxies::client::jude_client;
// pub use crate::core::proxies::server::jude_server;

pub mod core;
pub mod crypto;
pub mod util;

pub use tokio::task::AbortHandle;
pub use tokio_util::sync::CancellationToken;

#[frb(external)]
impl AbortHandle {
    pub fn abort(&self) {}
}

/*#[frb(sync)]
pub fn new_cancellation_token() -> Arc<CancellationToken> {
    Arc::new(CancellationToken::new())
}

#[frb(sync)]
pub fn cancel_cancellation_token(token: Arc<CancellationToken>) {
    token.cancel();
}*/

#[frb(external)]
impl CancellationToken {
    #[frb(sync)]
    pub fn new() -> CancellationToken {}
    #[frb(sync)]
    pub fn cancel(&self) {}
}

#[tokio::main(flavor = "current_thread")]
async fn test() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::proxies::client::{jude_client, jude_client_cancellable};
    use crate::core::proxies::server::{jude_server, jude_server_cancellable};
    use crate::session::Session;
    use crate::util::run_minecraft_vanilla_server::{
        run_minecraft_vanilla_server, run_minecraft_vanilla_server_cancellable,
    };
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use crate::util::logging::start_logger;

    #[tokio::test]
    async fn test_logger() {
        start_logger().unwrap();
        start_logger().unwrap();
    }

    #[tokio::test]
    async fn test_session() {
        let session = Session::new("wss://raspberrypi.tail38f7c6.ts.net:10000".to_owned())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test_client() {
        let session = Session::new("ws://127.0.0.1:5100".to_owned())
            .await
            .unwrap();
        let token = jude_client_cancellable(
            "test".to_owned(),
            session.clone(),
            "127.0.0.2:25565".to_owned(),
        );
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            token.cancel();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_server() {
        let token = run_minecraft_vanilla_server_cancellable(
            "/Users/leonardnolting/Documents/jude/servers/testserver/server.jar".to_owned(),
            "/Users/leonardnolting/Library/Application Support/gg.jude.jude/jude/java/jres/21/jdk-21.0.6+7-jre/Contents/Home/bin/java".to_owned(),
            3000
        );

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            token.cancel();
        })
        .await
        .unwrap();

        /*tokio::task::spawn_blocking(async {
            let token = run_minecraft_vanilla_server_cancellable(
                "/Users/leonardnolting/Documents/jude/servers/testserver/server.jar".to_owned(),
                "/Users/leonardnolting/Library/Application Support/gg.jude.jude/jude/java/jres/21/jdk-21.0.6+7-jre/Contents/Home/bin/java".to_owned(),
                3000
            );

            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                token.cancel();
            }).await.unwrap();
        }).await.unwrap();*/

        /*tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }).await.unwrap();*/

        /*
        let session = Session::new("ws://34.75.203.169:5100".to_owned())
            .await
            .unwrap();

        let token = jude_server_cancellable(
            "hyperpixel".to_owned(),
            session.clone(),
            "127.0.0.1:3000".to_owned(),
        );

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            token.cancel();
        })
        .await
        .unwrap();*/

        println!("Done!");

        // jude_client("hyperpixel".to_owned(), session, "127.0.0.1:3000".to_owned()).await.unwrap();
        // jude_server("hyperpixel".to_owned(), session, "127.0.0.1:3000".to_owned()).await.unwrap();
    }
}
