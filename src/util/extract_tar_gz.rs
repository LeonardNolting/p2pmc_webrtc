use flate2::read::GzDecoder;
use tar::Archive;

// TODO convert data to a Stream of bytes, to extract while downloading
// TODO allow cancellation
pub fn extract_tar_gz(data: &[u8], extraction_path: &str) -> Result<(), std::io::Error> {
    let tar = GzDecoder::new(data);
    let mut archive = Archive::new(tar);
    archive.unpack(extraction_path)?;

    Ok(())
}