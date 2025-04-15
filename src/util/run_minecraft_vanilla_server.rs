use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tokio::fs;
use tokio::process::Command;
use tracing::info;

pub async fn run_minecraft_vanilla_server(server: &str, java: &str, port: u16) {
    let server = Path::new(server);
    let server_file = server.file_name().unwrap();
    let server_dir = server.parent().unwrap();
    
    info!("Server file: {server_file:?}");
    info!("Server dir: {server_dir:?}");

    let mut perms = fs::metadata(java).await.unwrap().permissions();

    // Add execute permission (equivalent to chmod +x)
    perms.set_mode(perms.mode() | 0o111);

    // Set the new permissions
    fs::set_permissions(java, perms).await.unwrap();

    // Create the command using relative paths
    let status = Command::new(java)
        .current_dir(server_dir)  // Change working directory before executing
        .arg("-jar")
        .arg(server_file)
        .arg("nogui")
        .arg("-port")
        .arg(port.to_string())
        .status().await.unwrap();

    /*// Create the command using relative paths
    let status = Command::new("pwd")
        .status().await.unwrap();*/
    
    println!("Minecraft server status: {status}");

    if status.success() {
        println!("Minecraft server started successfully");
    } else {
        eprintln!("Failed to start Minecraft server");
        eprintln!("{}", status.code().unwrap().to_string());
    }
}