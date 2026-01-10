use anyhow::Result;
use blake3::Hasher;
use memmap2::MmapOptions;
use std::fs::File;
use std::io::{Read, copy};
use std::path::Path;
use crate::core::VeghMetadata;

// Compute BLAKE3 hash of a file
pub fn compute_file_hash(path: &Path) -> Result<String> {
    let file = File::open(path)?;
    // Try mmap first
    if let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
        let mut hasher = Hasher::new();
        hasher.update_rayon(&mmap);
        Ok(hasher.finalize().to_hex().to_string())
    } else {
        // Fallback
        let mut f = File::open(path)?;
        let mut hasher = Hasher::new();
        copy(&mut f, &mut hasher)?;
        Ok(hasher.finalize().to_hex().to_string())
    }
}

pub fn check_integrity(input: &Path) -> Result<(String, Option<VeghMetadata>)> {
    let hash = compute_file_hash(input)?;

    // Try to read meta
    let file = File::open(input)?;
    let meta = if let Ok(d) = zstd::stream::read::Decoder::new(file) {
        let mut ar = tar::Archive::new(d);
        ar.entries().ok().and_then(|mut entries| {
            entries.find_map(|e| {
                let mut entry = e.ok()?;
                if entry.path().ok()?.to_string_lossy() == ".vegh.json" {
                    let mut s = String::new();
                    entry.read_to_string(&mut s).ok()?;
                    serde_json::from_str(&s).ok()
                } else {
                    None
                }
            })
        })
    } else {
        None
    };

    Ok((hash, meta))
}
