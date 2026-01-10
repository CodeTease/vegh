use anyhow::{Context, Result};
use chrono::Utc;
use colored::*;
use futures::stream::{self, StreamExt};
use ignore::{WalkBuilder, overrides::OverrideBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom}; // Re-added Read for tar processing/chunks
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::SystemTime;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::storage::{FileCacheEntry, VeghCache, ManifestEntry, SnapshotManifest, load_cache, save_cache, CACHE_DIR};
use crate::hash::{compute_file_hash, compute_chunks};

// Files that should ALWAYS be included regardless of ignore rules
const PRESERVED_FILES: &[&str] = &[".veghignore", ".gitignore"];

// [FV3] Updated Version
const SNAPSHOT_FORMAT_VERSION: &str = "3";
// CDC Threshold: 1MB
const CDC_THRESHOLD: u64 = 1024 * 1024;
// CDC Avg Size: 1MB (min ~256KB, max ~4MB)
const CDC_AVG_SIZE: usize = 1024 * 1024;

#[derive(Serialize, Deserialize, Debug)]
pub struct VeghMetadata {
    pub author: String,
    pub timestamp: i64,
    #[serde(default)]
    pub timestamp_human: Option<String>,
    pub comment: String,
    pub tool_version: String,
    #[serde(default = "default_format_version")]
    pub format_version: String,
}

fn default_format_version() -> String {
    "1".to_string()
}

// Utility to format bytes into human-readable strings
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}

pub fn create_snap(
    source: &Path,
    output: &Path,
    level: i32,
    comment: Option<String>,
    include: Vec<String>,
    exclude: Vec<String>,
    no_cache: bool,
) -> Result<(u64, u64)> {
    // [NEW] Setup Atomic flag for Ctrl+C detection
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        println!("\n{} Interrupted! Stopping gracefully...", "🛑".red());
    });

    let mut cache = if no_cache {
        println!("{} Skipping cache (forced refresh)", "🔄".yellow());
        VeghCache::default()
    } else {
        load_cache(source)
    };

    let mut new_cache_files = HashMap::new();
    let file = File::create(output).context("Output file creation failed")?;
    let output_abs = fs::canonicalize(output).unwrap_or(output.to_path_buf());

    let meta = VeghMetadata {
        author: "CodeTease".to_string(),
        timestamp: Utc::now().timestamp(),
        timestamp_human: Some(Utc::now().to_rfc3339()),
        comment: comment.unwrap_or_default(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        format_version: SNAPSHOT_FORMAT_VERSION.to_string(),
    };
    let meta_json = serde_json::to_string_pretty(&meta)?;

    let mut encoder = zstd::stream::write::Encoder::new(file, level)?;
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    encoder.multithread(workers as u32)?;

    let mut tar = tar::Builder::new(encoder);

    // Meta (Hidden Header)
    let mut header = tar::Header::new_gnu();
    header.set_path(".vegh.json")?;
    header.set_size(meta_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, ".vegh.json", meta_json.as_bytes())?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );

    let mut count = 0;
    let mut cached_count = 0;
    let mut total_raw_size = 0;
    
    // FV3: Track manifest and blobs
    let mut manifest = SnapshotManifest::default();
    let mut written_blobs: HashSet<String> = HashSet::new();

    let mut override_builder = OverrideBuilder::new(source);
    for pattern in include {
        let _ = override_builder.add(&format!("!{}", pattern));
    }
    for pattern in exclude {
        let _ = override_builder.add(&pattern);
    }

    let mut builder = WalkBuilder::new(source);
    builder.filter_entry(|entry| {
        !entry.path().to_string_lossy().contains(CACHE_DIR)
    });

    let overrides = override_builder
        .build()
        .context("Failed to build override rules")?;

    let mut builder = WalkBuilder::new(source);
    for &f in PRESERVED_FILES {
        builder.add_custom_ignore_filename(f);
    }

    builder.hidden(true).git_ignore(true).overrides(overrides);

    for result in builder.build() {
        if !running.load(Ordering::SeqCst) {
            pb.finish_with_message("Interrupted!");
            
            // Merge partial results into original cache
            cache.files.extend(new_cache_files);
            
            // Save cache
            if !no_cache {
                 println!("{} Interrupted. Saving progress to cache...", "💾".blue());
                 if let Err(e) = save_cache(source, &cache) {
                     eprintln!("{} Failed to save cache: {}", "⚠️".yellow(), e);
                 }
            }

            return Err(anyhow::anyhow!("Process interrupted by user"));
        }

        if let Ok(entry) = result {
            let path = entry.path();
            if path.is_file() {
                if fs::canonicalize(path).is_ok_and(|abs| abs == output_abs) {
                    continue;
                }

                let name = path.strip_prefix(source).unwrap_or(path);
                let name_str = name.to_string_lossy().to_string();

                let metadata = path.metadata()?;
                let modified = metadata
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let size = metadata.len();
                
                // Permission mode (Unix only mostly)
                #[cfg(unix)]
                let mode = std::os::unix::fs::MetadataExt::mode(&metadata);
                #[cfg(not(unix))]
                let mode = 0o644;

                total_raw_size += size;

                // --- Deduplication Logic ---
                // We use CDC if file size > threshold.
                let use_cdc = size > CDC_THRESHOLD;
                
                let (hash, chunks_info, is_cached) = if let Some(cached_entry) = cache.files.get(&name_str) {
                     let is_hit = cached_entry.modified == modified 
                               && cached_entry.size == size 
                               && cached_entry.hash.is_some()
                               && (!use_cdc || cached_entry.chunks.is_some());
                     
                     if is_hit {
                         let cached_hash = cached_entry.hash.clone().unwrap();
                         // If cached and we use chunks, we assume cached chunks are valid.
                         // But we need to reconstruct chunks info (hash/offset/len) to write blobs if needed.
                         // Problem: Cache stores only hashes, not offsets.
                         // Solution: Rerun CDC to get offsets, but use cached hashes to avoid re-hashing content?
                         // Actually CDC is fast. Re-running CDC + Hashing is slow.
                         // If we hit cache, we know the file content is same.
                         // We can just re-run CDC + Hash (it will produce same result) OR trust cache.
                         // But we need to write chunks to tar if they are missing.
                         // To write chunks, we need to read file.
                         // So we assume here "Cache Hit" means "We know the hashes, but we might still need to read file to write blobs".
                         // Optimization: Check if all chunk hashes are already in `written_blobs`.
                         // If ALL chunks are written, we SKIP reading file! (Big Win for deduplication across files)
                         // If ANY chunk is missing, we must read file and extract that chunk.
                         
                         let cached_chunks = if use_cdc { cached_entry.chunks.clone().unwrap() } else { vec![cached_hash.clone()] };
                         
                         let all_written = cached_chunks.iter().all(|h| written_blobs.contains(h));
                         
                         if all_written {
                             // Zero-read optimization!
                             (cached_hash, None, true)
                         } else {
                             // We need to read file to extract missing chunks.
                             // Re-compute everything (safest)
                             // Or implement logic to just extract missing chunks?
                             // Re-computing is simpler and safe.
                             // But we claimed "Dedup (Cached)".
                             // Let's re-compute but we expect it to match.
                             // Actually, if we re-compute, we are doing the work.
                             // If we want to skip hashing, we need to trust cache.
                             // But we still need to run CDC to get offsets.
                             // FastCDC is fast. Hashing is slow.
                             // So: Run CDC. For each chunk, calculate hash? No, use cached list index?
                             // No, offsets might change if file changed (but we checked mtime/size).
                             // If mtime/size match, content is same.
                             // So: Run CDC. We get chunks C1, C2...
                             // We assume C1 corresponds to cached_chunks[0].
                             // We don't hash C1. We just use cached_chunks[0] as hash.
                             // Then we check `written_blobs`. If missing, write C1 data.
                             
                             // However, `compute_chunks` currently does hashing.
                             // Let's just re-run `compute_chunks` for now to be safe and simple.
                             // The "Cache Hit" logic in Vegh was mainly to avoid re-hashing for change detection.
                             // If we re-run, we lose that benefit if we hash again.
                             // But wait! If `all_written` is false, it means we haven't seen this content in this session.
                             // So we MUST write it. Hashing is unavoidable if we want to verify integrity or if we don't implement complex "Trust Cache but Read" logic.
                             // But wait, if it's in Cache (from previous run), we know the hash.
                             // We can write it without hashing, just using the hash from cache as filename.
                             // But we need to know WHERE the chunk is in the file.
                             // So we MUST run CDC (fast) to get offsets.
                             // So:
                             // 1. Run CDC (get offsets).
                             // 2. Don't hash content. Use cached_chunks[i] as hash.
                             // 3. Write blob if needed.
                             
                             // To implement this, we need a `compute_chunks_offsets_only` or similar?
                             // Or just use `compute_chunks` which does hashing.
                             // Given implementation time, let's just re-compute if we need to write blobs.
                             // It's safer.
                             // The "Zero-read optimization" above handles the case where chunks are already in tar (e.g. duplicate files).
                             if use_cdc {
                                let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                                (h, Some(chunks), true) // It was cached metadata-wise
                             } else {
                                let h = compute_file_hash(path)?;
                                (h, None, true)
                             }
                         }
                     } else {
                         // Cache Miss
                         if use_cdc {
                             let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                             (h, Some(chunks), false)
                         } else {
                             let h = compute_file_hash(path)?;
                             (h, None, false)
                         }
                     }
                } else {
                     // New file
                     if use_cdc {
                         let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                         (h, Some(chunks), false)
                     } else {
                         let h = compute_file_hash(path)?;
                         (h, None, false)
                     }
                };

                if is_cached && chunks_info.is_none() { 
                    // This happens if "Zero-read optimization" triggered
                    cached_count += 1;
                    pb.set_message(format!("Dedup (Skipped): {}", name.display()));
                    
                    // We need to retrieve chunks from cache to put in manifest
                    let cached_entry = cache.files.get(&name_str).unwrap();
                    let chunk_hashes = if use_cdc { 
                        cached_entry.chunks.clone().unwrap() 
                    } else { 
                        vec![cached_entry.hash.clone().unwrap()] 
                    };

                    manifest.entries.push(ManifestEntry {
                        path: name_str.clone(),
                        hash: hash.clone(),
                        size,
                        modified,
                        mode,
                        chunks: Some(chunk_hashes.clone()),
                    });
                    
                    // We also need to update new_cache
                    new_cache_files.insert(name_str.clone(), cached_entry.clone());

                } else {
                    if is_cached {
                         pb.set_message(format!("Dedup (Rescan): {}", name.display()));
                    } else {
                         pb.set_message(format!("Hashing: {}", name.display()));
                    }

                    let mut chunk_hashes_list = Vec::new();
                    
                    if let Some(chunks) = chunks_info {
                        // CDC Mode
                         let mut f = File::open(path)?;
                         
                         for chunk in chunks {
                             chunk_hashes_list.push(chunk.hash.clone());
                             
                             if !written_blobs.contains(&chunk.hash) {
                                 let blob_path = format!("blobs/{}", chunk.hash);
                                 
                                 // We need to read exact chunk.
                                 // Seek to offset
                                 f.seek(SeekFrom::Start(chunk.offset as u64))?;
                                 // Read length
                                 let mut chunk_data = vec![0u8; chunk.length];
                                 f.read_exact(&mut chunk_data)?;
                                 
                                 // Write to tar
                                 let mut header = tar::Header::new_gnu();
                                 header.set_path(&blob_path)?;
                                 header.set_size(chunk.length as u64);
                                 header.set_mode(0o644);
                                 header.set_cksum();
                                 tar.append_data(&mut header, &blob_path, &chunk_data[..])?;
                                 
                                 written_blobs.insert(chunk.hash);
                             }
                         }
                    } else {
                        // Whole File Mode
                        chunk_hashes_list.push(hash.clone());
                        if !written_blobs.contains(&hash) {
                            let blob_path = format!("blobs/{}", hash);
                            let mut f = File::open(path)?;
                            tar.append_file(&blob_path, &mut f)?;
                            written_blobs.insert(hash.clone());
                        }
                    }

                    // Update Cache
                    new_cache_files.insert(name_str.clone(), FileCacheEntry { 
                        size, 
                        modified, 
                        hash: Some(hash.clone()),
                        chunks: Some(chunk_hashes_list.clone())
                    });

                    // Update Manifest
                    manifest.entries.push(ManifestEntry {
                        path: name_str.clone(),
                        hash: hash.clone(),
                        size,
                        modified,
                        mode,
                        chunks: Some(chunk_hashes_list),
                    });
                }
                
                count += 1;
            }
        }
    }

    // Write Manifest
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_path("manifest.json")?;
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_json.as_bytes())?;

    // Update and Save Cache
    cache.files = new_cache_files;
    cache.last_snapshot = Utc::now().timestamp();
    if !no_cache {
        if let Err(e) = save_cache(source, &cache) {
            pb.println(format!(
                "{} Warning: Failed to save cache: {}",
                "⚠️".yellow(),
                e
            ));
        }
    }

    pb.finish_with_message(format!(
        "Packed {} files ({} cached, {} blobs) using {} threads.",
        count, cached_count, written_blobs.len(), workers
    ));

    let zstd_encoder = tar.into_inner()?;
    zstd_encoder.finish()?;

    let final_size = fs::metadata(output)?.len();
    Ok((total_raw_size, final_size))
}

pub fn restore_snap(input: &Path, out_dir: &Path) -> Result<()> {
    if !out_dir.exists() {
        fs::create_dir_all(out_dir)?;
    }

    let file = File::open(input).context("Open failed")?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);

    archive.unpack(out_dir)?;
    
    let manifest_path = out_dir.join("manifest.json");
    if !manifest_path.exists() {
        // Legacy FV2 support
        return Ok(());
    }

    let manifest_file = File::open(&manifest_path)?;
    let manifest: SnapshotManifest = serde_json::from_reader(manifest_file)?;
    
    let blobs_dir = out_dir.join("blobs");
    
    println!("{} Reconstructing files from blobs...", "🔨".cyan());
    let pb = ProgressBar::new(manifest.entries.len() as u64);
    
    for entry in manifest.entries {
        let dest_path = out_dir.join(&entry.path);
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Truncate/Create file
        let mut dest_file = File::create(&dest_path)?;

        let chunk_hashes = entry.chunks.unwrap_or_else(|| vec![entry.hash.clone()]);
        
        for chunk_hash in chunk_hashes {
            let blob_path = blobs_dir.join(&chunk_hash);
            if blob_path.exists() {
                let mut blob_file = File::open(&blob_path)?;
                std::io::copy(&mut blob_file, &mut dest_file)?;
            } else {
                 pb.println(format!("{} Missing blob {} for {}", "⚠️".yellow(), chunk_hash, entry.path));
            }
        }

        // Restore permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(entry.mode);
            fs::set_permissions(&dest_path, permissions)?;
        }
        
        pb.inc(1);
    }
    
    pb.finish_with_message("Restoration Complete!");

    // Cleanup
    let _ = fs::remove_file(manifest_path);
    let _ = fs::remove_dir_all(blobs_dir);
    // Also remove .vegh.json if it was unpacked
    let _ = fs::remove_file(out_dir.join(".vegh.json"));

    Ok(())
}

pub fn list_snap(input: &Path) -> Result<()> {
    let file = File::open(input).context("Open failed")?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);

    println!("{} Contents of {}:", "📂".cyan(), input.display());
    println!("{:-<50}", "-");
    
    let mut manifest_found = false;
    
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?;
        let path_str = path.to_string_lossy();
        
        if path_str == "manifest.json" {
             let manifest: SnapshotManifest = serde_json::from_reader(entry)?;
             for item in manifest.entries {
                 let chunks_count = item.chunks.as_ref().map(|c| c.len()).unwrap_or(1);
                 println!("{:<40} {:>8} bytes [Chunks: {}] [{}]", 
                    item.path, 
                    item.size, 
                    chunks_count,
                    item.hash.chars().take(8).collect::<String>()
                );
             }
             manifest_found = true;
             break; 
        } else if !path_str.starts_with("blobs/") && path_str != ".vegh.json" {
            println!("{:<40} {:>8} bytes", path.display(), entry.size());
        }
    }
    
    if !manifest_found {
        println!("(Legacy Archive or No Manifest found)");
    }
    
    Ok(())
}

// Chunked Upload Logic (Unchanged)
pub async fn send_file(
    path: &Path,
    url: &str,
    force_chunk: bool,
    auth_token: Option<String>,
) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("File not found: {}", path.display());
    }

    let metadata = path.metadata()?;
    let file_size = metadata.len();
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    println!("{} Target: {}", "🌐".cyan(), url);
    println!(
        "{} File: {} ({:.2} MB)",
        "📄".cyan(),
        filename,
        file_size as f64 / 1024.0 / 1024.0
    );

    if auth_token.is_some() {
        println!("{} Authentication: Enabled", "🔒".green());
    }

    const CHUNK_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

    if file_size < CHUNK_THRESHOLD && !force_chunk {
        println!("{} Mode: Streaming Direct Upload", "🌊".yellow());
        send_streaming(path, url, file_size, auth_token).await
    } else {
        println!("{} Mode: Concurrent Chunked Upload", "📦".yellow());
        send_chunked(path, url, file_size, &filename, auth_token).await
    }
}

pub async fn send_streaming(
    path: &Path,
    url: &str,
    file_size: u64,
    auth_token: Option<String>,
) -> Result<()> {
    let client = Client::new();
    let file = AsyncFile::open(path).await?;
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = reqwest::Body::wrap_stream(stream);

    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    pb.set_message("Streaming...");

    let mut request = client
        .post(url)
        .header("Content-Length", file_size) 
        .header("User-Agent", "CodeTease-Vegh/0.3.0");

    if let Some(token) = auth_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let response = request.body(body).send().await?;

    if response.status().is_success() {
        pb.finish_with_message("Upload success!");
        let response_text = response
            .text()
            .await
            .unwrap_or_else(|_| "No response text".to_string());
        println!(
            "\n{} Server Response:\n{}",
            "📩".blue(),
            response_text.dimmed()
        );
        Ok(())
    } else {
        pb.abandon();
        anyhow::bail!("Upload failed with status: {}", response.status());
    }
}

pub async fn send_chunked(
    path: &Path,
    url: &str,
    file_size: u64,
    filename: &str,
    auth_token: Option<String>,
) -> Result<()> {
    const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
    let chunk_size_u64 = CHUNK_SIZE as u64;
    let total_chunks = file_size.div_ceil(chunk_size_u64);

    let pb = ProgressBar::new(total_chunks);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} chunks ({eta})")
            .unwrap()
            .progress_chars("█▒░"),
    );

    let client = Client::new();
    let chunks: Vec<u64> = (0..total_chunks).collect();

    let stream = stream::iter(chunks)
        .map(|i| {
            let client = client.clone();
            let url = url.to_string();
            let path = path.to_path_buf();
            let filename = filename.to_string();
            let pb = pb.clone();
            let auth_token = auth_token.clone();

            async move {
                let start = i * chunk_size_u64;
                let mut end = start + chunk_size_u64;
                if end > file_size {
                    end = file_size;
                }
                let current_chunk_size = (end - start) as usize;

                let mut file = AsyncFile::open(&path)
                    .await
                    .context("Failed to open file")?;
                file.seek(SeekFrom::Start(start))
                    .await
                    .context("Failed to seek")?;

                let mut buffer = vec![0u8; current_chunk_size];
                file.read_exact(&mut buffer)
                    .await
                    .context("Failed to read chunk")?;

                let mut request = client
                    .post(&url)
                    .header("X-File-Name", &filename)
                    .header("X-Chunk-Index", i.to_string())
                    .header("X-Total-Chunks", total_chunks.to_string())
                    .header("User-Agent", "CodeTease-Vegh/0.3.0");

                if let Some(token) = &auth_token {
                    request = request.header("Authorization", format!("Bearer {}", token));
                }

                match request.body(buffer).send().await {
                    Ok(res) => {
                        if res.status().is_success() {
                            pb.inc(1);
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!("Chunk {} failed: {}", i, res.status()))
                        }
                    }
                    Err(e) => Err(anyhow::anyhow!("Network error chunk {}: {}", i, e)),
                }
            }
        })
        .buffer_unordered(4); 

    let results: Vec<Result<()>> = stream.collect().await;
    for res in results {
        if let Err(e) = res {
            anyhow::bail!("Upload incomplete. Error: {}", e);
        }
    }

    pb.finish_with_message("All chunks sent successfully!");
    Ok(())
}
