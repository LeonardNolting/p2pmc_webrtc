pub use crate::core::p2p::session;
use flutter_rust_bridge::frb;
use std::sync::Arc;
// pub use crate::core::proxies::client::jude_client;
// pub use crate::core::proxies::server::jude_server;

pub mod core;
pub mod crypto;
pub mod util;

pub use tokio::task::AbortHandle;

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

#[tokio::main(flavor = "current_thread")]
async fn test() {}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::session::Session;
    use crate::util::logging::start_logger;
    use crate::util::crypto;
    
    #[tokio::test]
    async fn old_crypto_code() {
        // let (root_certified_key, root_age_key) = crypto::create_root().await.unwrap();
        let root_certified_key = crypto::load_root().await.unwrap();

        let is_client = std::env::args().nth(1).expect("Server or client?") == "client";
        let id = std::env::args().nth(2).expect("Provide an ID");

        let (user_certified_key, user_age_key) = crypto::create_user(id.clone(), &root_certified_key).await.unwrap();
        let user_rtc_cert = crypto::load_user(id.clone()).await.unwrap();

        // crypto::parse_cert(user_certified_key.cert.der()).await.unwrap();

        if is_client {
            let port = std::env::args().nth(3).unwrap_or("25565".to_string()).parse::<u16>().expect("Port must be a number");
            // start_client_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert, root_certified_key.cert.der().to_vec()).await;
        } else {
            let port = std::env::args().nth(3).expect("Provide a port on which a Minecraft server runs").parse::<u16>().expect("Port must be a number");
            // start_server_proxy(SIGNALING_SERVER, id.as_str(), port, user_rtc_cert).await;
        }
    }

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
