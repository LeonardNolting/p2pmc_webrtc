pub use crate::core::p2p::session as session;
// pub use crate::core::proxies::client::jude_client;
// pub use crate::core::proxies::server::jude_server;

pub mod util;
pub mod core;
pub mod crypto;

#[cfg(test)]
mod tests {
    use crate::core::proxies::server::jude_server;
    use crate::session::Session;
    use super::*;

    #[tokio::test]
    async fn test_server() {
        let session = Session::new("ws://34.75.203.169:5100".to_owned()).await.unwrap();
        jude_server("hyperpixel".to_owned(), &session, "127.0.0.1:3000").await.unwrap();
    }
}