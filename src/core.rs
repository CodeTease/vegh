use anyhow::{Context, Result};
use chrono::Utc;
use colored::*;
use futures::stream::{self, StreamExt};
use ignore::{WalkBuilder, overrides::OverrideBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom}; 
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::SystemTime;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::storage::{FileCacheEntry, CacheDB, ManifestEntry, SnapshotManifest, CACHE_DIR};
use crate::hash::{compute_file_hash, compute_chunks, compute_sparse_hash};

// Files that should ALWAYS be included regardless of ignore rules
const PRESERVED_FILES: &[&str] = &[".veghignore", ".gitignore"];

// [FV3] Updated Version
const SNAPSHOT_FORMAT_VERSION: &str = "3";
// CDC Threshold: 1MB
const CDC_THRESHOLD: u64 = 1024 * 1024;
// CDC Avg Size: 1MB (min ~256KB, max ~4MB)
const CDC_AVG_SIZE: usize = 1024 * 1024;
// Cache Retention: 30 Days
const CACHE_RETENTION_SEC: u64 = 30 * 24 * 60 * 60;

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
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        println!("\n{} Interrupted! Stopping gracefully...", "🛑".red());
    });

    let mut cache_db = if no_cache {
        println!("{} Skipping cache (forced refresh)", "🔄".yellow());
        CacheDB::open(source)?
    } else {
        CacheDB::open(source)?
    };

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
    let mut written_blobs: HashSet<String> = HashSet::new(); // Hex strings for easy lookup of written status

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
            
            // Sync partial results
            if !no_cache {
                 if let Err(e) = cache_db.sync_partial() {
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
                
                // --- Extended Metadata (Unix) ---
                #[cfg(unix)]
                let (mode, inode, device_id, ctime_sec, ctime_nsec) = {
                    use std::os::unix::fs::MetadataExt;
                    (
                        metadata.mode(), 
                        metadata.ino(), 
                        metadata.dev(), 
                        metadata.ctime(), 
                        metadata.ctime_nsec() as u32
                    )
                };
                #[cfg(not(unix))]
                let (mode, inode, device_id, ctime_sec, ctime_nsec) = (0o644, 0, 0, 0, 0);


                total_raw_size += size;
                
                let now_ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

                // --- Deduplication Logic ---
                let use_cdc = size > CDC_THRESHOLD;
                
                // Compute sparse hash eagerly
                let sparse_hash = compute_sparse_hash(path, size).ok();

                let cached_entry_opt = if no_cache { None } else { cache_db.get(&name_str)? };

                let (hash, chunks_info, is_cached) = if let Some(cached_entry) = cached_entry_opt {
                     // 1. Standard Path Match - Enhanced Check
                     let mut is_hit = cached_entry.modified == modified 
                               && cached_entry.size == size
                               && cached_entry.inode == inode
                               // Stricter check if platform supports it (non-zero in cache and current)
                               && (cached_entry.device_id == 0 || device_id == 0 || cached_entry.device_id == device_id)
                               && (cached_entry.ctime_sec == 0 || ctime_sec == 0 || cached_entry.ctime_sec == ctime_sec) 
                               && cached_entry.hash.is_some()
                               && (!use_cdc || cached_entry.chunks.is_some());
                     
                     // Sparse Verification
                     if is_hit {
                         if let Some(cached_sparse) = &cached_entry.sparse_hash {
                             if let Some(current_sparse) = &sparse_hash {
                                 if cached_sparse != current_sparse {
                                     is_hit = false;
                                     pb.set_message(format!("Mismatch (Sparse): {}", name.display()));
                                 }
                             }
                         }
                     }

                     if is_hit {
                         let cached_hash = cached_entry.hash.clone().unwrap();
                         // Convert chunks for checking existence
                         let cached_chunks = if use_cdc { 
                             cached_entry.chunks.clone().unwrap() 
                         } else { 
                             vec![cached_hash.clone()] 
                         };
                         
                         let all_written = cached_chunks.iter().all(|h| written_blobs.contains(&hex::encode(h)));
                         
                         if all_written {
                             (cached_hash, None, true)
                         } else {
                             // Re-read file if blobs missing
                             if use_cdc {
                                let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                                (h, Some(chunks), true)
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
                     // 2. Rename Detection
                     let mut renamed_entry: Option<FileCacheEntry> = None;
                     if !no_cache && inode > 0 {
                         if let Some(old_path) = cache_db.get_path_by_inode(inode)? {
                             if let Some(entry) = cache_db.get(&old_path)? {
                                 if entry.size == size 
                                    && entry.modified == modified 
                                    && entry.hash.is_some()
                                    && entry.device_id == device_id 
                                    && entry.ctime_sec == ctime_sec
                                 {
                                     renamed_entry = Some(entry);
                                 }
                             }
                         }
                     }

                     if let Some(entry) = renamed_entry {
                         let cached_hash = entry.hash.clone().unwrap_or_default();
                         let cached_chunks = if use_cdc { 
                             entry.chunks.clone().unwrap_or_else(|| vec![cached_hash.clone()]) 
                         } else { 
                             vec![cached_hash.clone()] 
                         };
                         let all_written = cached_chunks.iter().all(|h| written_blobs.contains(&hex::encode(h)));
                         
                         if all_written {
                             (cached_hash, None, true)
                         } else {
                             if use_cdc {
                                let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                                (h, Some(chunks), true)
                             } else {
                                let h = compute_file_hash(path)?;
                                (h, None, true)
                             }
                         }
                     } else {
                         if use_cdc {
                             let (h, chunks) = compute_chunks(path, CDC_AVG_SIZE)?;
                             (h, Some(chunks), false)
                         } else {
                             let h = compute_file_hash(path)?;
                             (h, None, false)
                         }
                     }
                };

                // Processing
                if is_cached && chunks_info.is_none() { 
                    cached_count += 1;
                    pb.set_message(format!("Dedup (Skipped): {}", name.display()));
                    
                    let entry_to_use_opt = if !no_cache {
                        if let Some(e) = cache_db.get(&name_str)? {
                            Some(e)
                        } else if inode > 0 {
                            if let Some(old_path) = cache_db.get_path_by_inode(inode)? {
                                cache_db.get(&old_path)?
                            } else { None }
                        } else { None }
                    } else { None };
                    
                    let mut final_entry = entry_to_use_opt.expect("Logic Error: marked cached but not found");
                    
                    // Hexify chunks for manifest
                    let chunk_hashes_bin = if use_cdc {
                        final_entry.chunks.clone().unwrap_or_else(|| vec![final_entry.hash.clone().unwrap_or_default()])
                    } else {
                        vec![final_entry.hash.clone().unwrap_or_default()]
                    };
                    
                    let chunk_hashes_hex: Vec<String> = chunk_hashes_bin.iter().map(|h| hex::encode(h)).collect();

                    manifest.entries.push(ManifestEntry {
                        path: name_str.clone(),
                        hash: hex::encode(hash),
                        size,
                        modified,
                        mode,
                        chunks: Some(chunk_hashes_hex),
                    });
                    
                    // Update entry metadata
                    if final_entry.sparse_hash.is_none() {
                        final_entry.sparse_hash = sparse_hash.clone();
                    }
                    final_entry.inode = inode;
                    final_entry.device_id = device_id;
                    final_entry.ctime_sec = ctime_sec;
                    final_entry.ctime_nsec = ctime_nsec;
                    final_entry.last_seen = now_ts;
                    
                    cache_db.insert(&name_str, &final_entry)?;

                } else {
                    if is_cached {
                         pb.set_message(format!("Dedup (Rescan): {}", name.display()));
                    } else {
                         pb.set_message(format!("Hashing: {}", name.display()));
                    }

                    let mut chunk_hashes_bin = Vec::new();
                    
                    if let Some(chunks) = chunks_info {
                        // CDC Mode
                         let mut f = File::open(path)?;
                         
                         for chunk in chunks {
                             chunk_hashes_bin.push(chunk.hash.clone());
                             let chunk_hex = hex::encode(chunk.hash);
                             
                             if !written_blobs.contains(&chunk_hex) {
                                 let blob_path = format!("blobs/{}", chunk_hex);
                                 
                                 f.seek(SeekFrom::Start(chunk.offset as u64))?;
                                 let mut chunk_data = vec![0u8; chunk.length];
                                 f.read_exact(&mut chunk_data)?;
                                 
                                 let mut header = tar::Header::new_gnu();
                                 header.set_path(&blob_path)?;
                                 header.set_size(chunk.length as u64);
                                 header.set_mode(0o644);
                                 header.set_cksum();
                                 tar.append_data(&mut header, &blob_path, &chunk_data[..])?;
                                 
                                 written_blobs.insert(chunk_hex);
                             }
                         }
                    } else {
                        // Whole File Mode
                        chunk_hashes_bin.push(hash.clone());
                        let hash_hex = hex::encode(hash);
                        
                        if !written_blobs.contains(&hash_hex) {
                            let blob_path = format!("blobs/{}", hash_hex);
                            let mut f = File::open(path)?;
                            tar.append_file(&blob_path, &mut f)?;
                            written_blobs.insert(hash_hex);
                        }
                    }

                    // Insert into Cache DB
                    cache_db.insert(&name_str, &FileCacheEntry { 
                        size, 
                        modified,
                        inode,
                        device_id,
                        ctime_sec,
                        ctime_nsec,
                        last_seen: now_ts,
                        hash: Some(hash.clone()),
                        chunks: Some(chunk_hashes_bin.clone()),
                        sparse_hash: sparse_hash.clone(),
                    })?;

                    // Update Manifest
                    manifest.entries.push(ManifestEntry {
                        path: name_str.clone(),
                        hash: hex::encode(hash),
                        size,
                        modified,
                        mode,
                        chunks: Some(chunk_hashes_bin.iter().map(|h| hex::encode(h)).collect()),
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

    // GC & Commit Cache
    if !no_cache {
        println!("{} Running garbage collection...", "🧹".blue());
        if let Err(e) = cache_db.garbage_collect_and_merge(CACHE_RETENTION_SEC) {
            eprintln!("{} GC Warning: {}", "⚠️".yellow(), e);
        }

        if let Err(e) = cache_db.commit() {
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
