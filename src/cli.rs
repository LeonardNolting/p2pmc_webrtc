use std::sync::Arc;
use clap::{Parser, Subcommand};
use tracing::info;
use crate::{jude_client, jude_server};
use crate::p2p::session::Session;

#[derive(Parser)]
struct Cli {
    #[clap(short, long, help = "Identifier for this peer")]
    id: String,
    #[clap(
        short,
        long,
        default_value = "ws://127.0.0.1:5100",
        help = "Signaling server URL"
    )]
    signaling_server: String,

    #[clap(subcommand)]
    command: crate::Command,
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
    let cli = crate::Cli::parse();

    info!(id = cli.id, "Starting jude as {}", cli.id);

    let session = Arc::new(Session::new(cli.signaling_server.to_string()).await?);

    match cli.command {
        crate::Command::Server { minecraft_server } => {
            jude_server(cli.id, session, &minecraft_server).await
        }
        crate::Command::Client { minecraft_adapter } => {
            jude_client(cli.id, session, &minecraft_adapter).await
        }
    }
}