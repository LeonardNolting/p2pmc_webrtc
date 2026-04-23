use std::fs::File;
use std::path::Path;
use flate2::read::GzDecoder;
use tar::Archive;
use tracing::info;

// TODO convert data to a Stream of bytes, to extract while downloading
// TODO allow cancellation
pub fn extract_tar_gz(data: &[u8], extraction_path: &str) -> Result<(), std::io::Error> {
    let tar = GzDecoder::new(data);
    let mut archive = Archive::new(tar);
    archive.unpack(extraction_path)?;

    Ok(())
}

/// Extracts a .tar.gz file from the local disk to a specified destination.
pub fn extract_tar_gz_from_file(src_path: String, dest_path: String) -> Result<(), String> {
    info!("Extracting tar.gz file from {} to {}", src_path, dest_path);
    // 1. Open the source file
    let file = File::open(&src_path)
        .map_err(|e| format!("Failed to open source file '{}': {}", src_path, e))?;

    // 2. Chain the decoder and the archive parser
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);

    // 3. Unpack to the destination
    archive.unpack(&dest_path)
        .map_err(|e| format!("Failed to unpack to '{}': {}", dest_path, e))?;

    Ok(())
}