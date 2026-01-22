use crate::core::VeghMetadata;
use anyhow::Result;
use blake3::Hasher;
use memmap2::MmapOptions;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, copy};
use std::path::Path;

// Trust-but-Verify: Sparse Hashing
// Reads 4KB head + 4KB tail (or less if small)
pub fn compute_sparse_hash(path: &Path, size: u64) -> Result<[u8; 32]> {
    let mut file = File::open(path)?;
    let mut hasher = Hasher::new();
    let sample_size = 4096;

    if size <= (sample_size * 2) {
        // File too small, hash everything
        copy(&mut file, &mut hasher)?;
    } else {
        // Head
        let mut buffer = vec![0u8; sample_size as usize];
        file.read_exact(&mut buffer)?;
        hasher.update(&buffer);

        // Tail
        file.seek(SeekFrom::End(-(sample_size as i64)))?;
        file.read_exact(&mut buffer)?;
        hasher.update(&buffer);
    }

    Ok(*hasher.finalize().as_bytes())
}

// Compute BLAKE3 hash of a file
pub fn compute_file_hash(path: &Path) -> Result<[u8; 32]> {
    let file = File::open(path)?;
    // Try mmap first
    if let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
        let mut hasher = Hasher::new();
        hasher.update_rayon(&mmap);
        Ok(*hasher.finalize().as_bytes())
    } else {
        // Fallback
        let mut f = File::open(path)?;
        let mut hasher = Hasher::new();
        copy(&mut f, &mut hasher)?;
        Ok(*hasher.finalize().as_bytes())
    }
}

pub struct ChunkInfo {
    pub hash: [u8; 32],
    pub offset: usize,
    pub length: usize,
}

// Compute chunks using FastCDC and their hashes
pub fn compute_chunks(path: &Path, avg_size: usize) -> Result<([u8; 32], Vec<ChunkInfo>)> {
    let file = File::open(path)?;
    let mut chunks = Vec::new();
    let mut file_hasher = Hasher::new();

    // Use mmap for CDC
    if let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
        // Calculate chunks
        let cut = fastcdc::v2020::FastCDC::new(
            &mmap,
            (avg_size / 2) as u32,
            avg_size as u32,
            (avg_size * 2) as u32,
        );

        for chunk in cut {
            let chunk_data = &mmap[chunk.offset..chunk.offset + chunk.length];
            let mut chunk_hasher = Hasher::new();
            chunk_hasher.update(chunk_data);
            let chunk_hash = *chunk_hasher.finalize().as_bytes();

            chunks.push(ChunkInfo {
                hash: chunk_hash,
                offset: chunk.offset,
                length: chunk.length,
            });

            // Also update file hash
            file_hasher.update(chunk_data);
        }

        Ok((*file_hasher.finalize().as_bytes(), chunks))
    } else {
        // Fallback for non-mmap (e.g. pipe, special files) - treating as single chunk
        // CDC on stream is possible but complex to implement here without duplicating logic.
        // For simplicity, treat as one chunk.
        let mut f = File::open(path)?;
        let mut hasher = Hasher::new();
        copy(&mut f, &mut hasher)?;
        let hash = *hasher.finalize().as_bytes();

        // Return whole file as one chunk
        let len = file.metadata()?.len() as usize;
        chunks.push(ChunkInfo {
            hash,
            offset: 0,
            length: len,
        });
        Ok((hash, chunks))
    }
}

pub fn check_integrity(input: &Path) -> Result<(String, Option<VeghMetadata>)> {
    let hash_bytes = compute_file_hash(input)?;
    let hash = hex::encode(hash_bytes);

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
