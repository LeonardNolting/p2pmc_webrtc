use std::sync::Arc;
use clap::{Parser, Subcommand};
use tracing::info;
use crate::core::p2p::session::Session;
use crate::core::proxies::client::jude_client;
use crate::core::proxies::server::jude_server;

#[derive(Parser)]
struct Cli {
    #[clap(short, long, help = "Identifier for this peer")]
    id: String,
    #[clap(
        short,
        long,
        default_value = "ws://34.75.203.169:5100",
        help = "Signaling server URL"
    )]
    signaling_server: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[clap(short, long, default_value = "127.0.0.1:3000")]
        minecraft_server: String,
    },
    Client {
        // Use 127.0.0.2 as this is less likely to be DNS filtered
        #[clap(short, long, default_value = "127.0.0.2:25565")]
        minecraft_adapter: String,
    },
}

pub async fn cli() -> anyhow::Result<()> {
    let cli = Cli::parse();

    info!(id = cli.id, "Starting jude as {}", cli.id);

    let session = Arc::new(Session::new(cli.signaling_server.to_string()).await?);

    match cli.command {
        Command::Server { minecraft_server } => {
            jude_server(cli.id, session, &minecraft_server).await
        }
        Command::Client { minecraft_adapter } => {
            jude_client(cli.id, session, &minecraft_adapter).await
        }
    }
}