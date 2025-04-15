use cancellable::cancellable;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::fs;
use tokio::process::Command;
use tracing::{error, info, instrument};

#[instrument(name = "minecraft_server", skip_all)]
#[cancellable]
pub async fn run_minecraft_vanilla_server(server: String, java: String, port: u16) {
    info!(server, java, port, "Starting Minecraft server");

    let server = Path::new(&server);
    let server_file = server.file_name().unwrap();
    let server_dir = server.parent().unwrap();

    let mut perms = fs::metadata(&java).await.expect("Receiving metadata failed").permissions();

    // Add execute permission (equivalent to chmod +x)
    perms.set_mode(perms.mode() | 0o111);

    // Set the new permissions
    fs::set_permissions(&java, perms).await.expect("Marking java binary as executable failed");

    // Create the command using relative paths
    let status = Command::new(&java)
        .current_dir(server_dir) // Change working directory before executing
        .arg("-jar")
        .arg(server_file)
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
