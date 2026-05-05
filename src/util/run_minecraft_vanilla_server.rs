use cancellable::cancellable;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use tokio::fs;
use std::path::Path;
use tokio::process::Command;
use tracing::{error, info, instrument};

async fn mark_executable(path: &str) {
    #[cfg(unix)]
    {
        let mut perms = fs::metadata(&path).await.expect("Receiving metadata failed").permissions();

        // Add execute permission (equivalent to chmod +x)
        perms.set_mode(perms.mode() | 0o111);

        // Set the new permissions
        fs::set_permissions(&path, perms).await.expect("Marking java binary as executable failed");
    }
}

#[instrument(name = "minecraft_server", skip_all)]
#[cancellable]
pub async fn run_minecraft_vanilla_server(directory: String, server_jar: String, java: String, port: u16) {
    info!(directory, server_jar, java, port, "Starting Minecraft server");

    mark_executable(&java).await;

    let status = Command::new(&java)
        .current_dir(directory) // Change working directory before executing
        .arg("-jar")
        .arg(server_jar)
        .arg("nogui")
        .arg("-port")
        .arg(port.to_string())
        .kill_on_drop(true)
        .status().await
        .expect("Running java command failed");

    info!("Minecraft server closed with status {status}");

    if status.success() {
        info!("Minecraft server closed successfully");
    } else {
        error!(code = status.code().unwrap(), "Minecraft server execution ended unsuccessfully");
    }
}
