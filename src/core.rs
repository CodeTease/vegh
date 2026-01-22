use anyhow::{Context, Result};
use chrono::Utc;
use colored::*;
use crossbeam_channel::bounded;
use futures::stream::{self, StreamExt};
use ignore::{WalkBuilder, overrides::OverrideBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::SystemTime;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::hash::{compute_chunks, compute_file_hash, compute_sparse_hash};
use crate::storage::{CACHE_DIR, CacheDB, FileCacheEntry, ManifestEntry, SnapshotManifest, StoredChunk};

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
// Batch Commit Size (Files)
const BATCH_COMMIT_SIZE: usize = 1000;

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

// Pipeline Messages
enum WorkerResult {
    Processed(ProcessedMessage),
    Error(String),
}

struct ProcessedMessage {
    path_str: String,
    abs_path: PathBuf,
    metadata_info: MetadataInfo,
    entry: FileCacheEntry,
    data_action: DataAction,
    is_cached_hit: bool,
}

struct MetadataInfo {
    size: u64,
    modified: u64,
    mode: u32,
}

enum DataAction {
    Cached,                      // All data in cache/blobs already
    WriteFile(Vec<u8>),          // Hash bytes
    WriteChunks(Vec<StoredChunk>), // List of chunks to write
}

// We reuse StoredChunk from storage.rs

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
    // We use workers for hashing, so let zstd use multithreading too
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    encoder.multithread(num_threads as u32)?;

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

    // 1. Setup Channels
    let (path_tx, path_rx) = bounded::<PathBuf>(1024);
    let (res_tx, res_rx) = bounded::<WorkerResult>(1024);

    // 2. Scanner Thread
    let source_buf = source.to_path_buf();
    let source_buf_for_scan = source_buf.clone();
    let path_tx_for_scan = path_tx.clone();
    let r_scan = running.clone();
    let scanner_handle = std::thread::spawn(move || {
        let mut override_builder = OverrideBuilder::new(&source_buf_for_scan);
        for pattern in include {
            let _ = override_builder.add(&format!("!{}", pattern));
        }
        for pattern in exclude {
            let _ = override_builder.add(&pattern);
        }

        let overrides = match override_builder.build() {
            Ok(o) => o,
            Err(_) => return, // Should log?
        };

        let mut builder = WalkBuilder::new(&source_buf_for_scan);
        for &f in PRESERVED_FILES {
            builder.add_custom_ignore_filename(f);
        }
        builder.filter_entry(|entry| !entry.path().to_string_lossy().contains(CACHE_DIR));
        builder.hidden(true).git_ignore(true).overrides(overrides);

        for result in builder.build() {
            if !r_scan.load(Ordering::SeqCst) {
                break;
            }
            if let Ok(entry) = result
                && entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    if let Ok(abs) = fs::canonicalize(entry.path())
                        && abs == output_abs {
                            continue;
                        }
                    if path_tx_for_scan.send(entry.path().to_path_buf()).is_err() {
                        break;
                    }
                }
        }
    });

    // 3. Worker Threads
    let mut worker_handles = Vec::new();
    let cache_reader = cache_db.reader();
    let written_blobs = Arc::new(dashmap::DashMap::new());

    let written_blobs_shared = written_blobs.clone();
    let _source_path_str = source.to_string_lossy().to_string();

    for _ in 0..num_threads {
        let rx = path_rx.clone();
        let tx = res_tx.clone();
        let reader = cache_reader.clone();
        let blobs = written_blobs_shared.clone();
        let src_root = source_buf.clone();
        let r_worker = running.clone();

        worker_handles.push(std::thread::spawn(move || {
            while let Ok(path) = rx.recv() {
                if !r_worker.load(Ordering::SeqCst) {
                    break;
                }

                // Process File
                let process_res = (|| -> Result<ProcessedMessage> {
                    let name = path.strip_prefix(&src_root).unwrap_or(&path);
                    let name_str = name.to_string_lossy().to_string();
                    let metadata = path.metadata()?;
                    let size = metadata.len();
                    let modified = metadata
                        .modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    #[cfg(unix)]
                    let (mode, inode, device_id, ctime_sec, ctime_nsec) = {
                        use std::os::unix::fs::MetadataExt;
                        (
                            metadata.mode(),
                            metadata.ino(),
                            metadata.dev(),
                            metadata.ctime(),
                            metadata.ctime_nsec() as u32,
                        )
                    };
                    #[cfg(windows)]
                    let (mode, inode, device_id, ctime_sec, ctime_nsec) = {
                        // Unstable features (file_index, volume_serial_number) replaced with 0
                        // to ensure stable compilation.
                        (0o644, 0, 0, 0, 0)
                    };
                    #[cfg(not(any(unix, windows)))]
                    let (mode, inode, device_id, ctime_sec, ctime_nsec) = (0o644, 0, 0, 0, 0);

                    let now_ts = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let use_cdc = size > CDC_THRESHOLD;

                    let cached_entry_opt = if no_cache {
                        None
                    } else {
                        reader.get(&name_str)?
                    };

                    let (hash, chunks_info, is_cached_hit) = if let Some(ref cached_entry) = cached_entry_opt {
                        let is_hit = cached_entry.modified == modified
                            && cached_entry.size == size
                            && cached_entry.inode == inode
                            && (cached_entry.device_id == 0
                                || device_id == 0
                                || cached_entry.device_id == device_id)
                            && (cached_entry.ctime_sec == 0
                                || ctime_sec == 0
                                || cached_entry.ctime_sec == ctime_sec) 
                            && cached_entry.hash.is_some();
                        
                        // If metadata matches, we consider it a hit.
                        if is_hit {
                             // Check if we have chunks if CDC is expected
                            if use_cdc {
                                if let Ok(Some(chunks)) = cached_entry.get_chunks() {
                                    // We have chunks, great!
                                    (cached_entry.hash.unwrap(), Some(chunks), true)
                                } else {
                                    // Metadata matches but we don't have valid chunks (maybe migrated from old format)
                                    // We must recompute.
                                    let (h, chunks) = compute_chunks(&path, CDC_AVG_SIZE)?;
                                    
                                    // Convert hash::ChunkInfo to storage::StoredChunk
                                    let stored_chunks: Vec<StoredChunk> = chunks.into_iter().map(|c| StoredChunk {
                                        hash: c.hash,
                                        offset: c.offset as u64,
                                        length: c.length as u32,
                                    }).collect();
                                    
                                    (h, Some(stored_chunks), false)
                                }
                            } else {
                                // No CDC, just simple hash
                                (cached_entry.hash.unwrap(), None, true)
                            }
                        } else {
                            // Metadata mismatch -> Recompute
                             if use_cdc {
                                let (h, chunks) = compute_chunks(&path, CDC_AVG_SIZE)?;
                                let stored_chunks: Vec<StoredChunk> = chunks.into_iter().map(|c| StoredChunk {
                                    hash: c.hash,
                                    offset: c.offset as u64,
                                    length: c.length as u32,
                                }).collect();
                                (h, Some(stored_chunks), false)
                            } else {
                                let h = compute_file_hash(&path)?;
                                (h, None, false)
                            }
                        }
                    } else {
                         // Not in cache -> Compute
                        if use_cdc {
                            let (h, chunks) = compute_chunks(&path, CDC_AVG_SIZE)?;
                            let stored_chunks: Vec<StoredChunk> = chunks.into_iter().map(|c| StoredChunk {
                                    hash: c.hash,
                                    offset: c.offset as u64,
                                    length: c.length as u32,
                                }).collect();
                            (h, Some(stored_chunks), false)
                        } else {
                            let h = compute_file_hash(&path)?;
                            (h, None, false)
                        }
                    };
                    
                    // Note: We REMOVED the "is content already in blobs?" check from the Cache Hit logic.
                    // That check now happens ONLY when deciding whether to write data (DataAction).
                    // This satisfies "Point 1: Validation Point Shift".

                    // Construct Result
                    let mut data_action = DataAction::Cached;

                    if let Some(chunks) = chunks_info.clone() { // Clone needed? chunks_info is moved into entry later if we are not careful
                        let mut chunks_to_write = Vec::new();
                        for c in chunks {
                            let hex_h = hex::encode(c.hash);
                            // Check if content needs to be written (Deduplication)
                            if !blobs.contains_key(&hex_h) {
                                chunks_to_write.push(c);
                            }
                        }
                        if !chunks_to_write.is_empty() {
                            data_action = DataAction::WriteChunks(chunks_to_write);
                        }
                    } else {
                        let hex_h = hex::encode(hash);
                        if !blobs.contains_key(&hex_h) {
                            data_action = DataAction::WriteFile(hash.to_vec());
                        }
                    }

                    // Create Entry
                    let mut entry = FileCacheEntry {
                        size,
                        modified,
                        inode,
                        device_id,
                        ctime_sec,
                        ctime_nsec,
                        last_seen: now_ts,
                        hash: Some(hash),
                        chunks_compressed: None, // Set below
                        sparse_hash: None, // We stopped computing sparse hash for every file to match "Skip Compute"
                    };
                    
                    // Should we compute sparse hash for new files?
                    // The prompt says "Trust-but-Verify... Mismatches trigger full re-hash".
                    // But also "Totally skip step calculation BLAKE3".
                    // If we skip calculation, we can't verify unless we do sparse hash.
                    // The prompt says: "Only Metadata... matches, you must mark as Cache Hit immediately".
                    // So I will skip sparse hash computation for Hits.
                    // For new files, I should probably still compute it if I want to store it for future verification (if enabled).
                    // But the logic "Get Result, Skip Compute" implies we trust metadata fully.
                    // I will leave sparse_hash as None or compute it only for new files?
                    // Existing code computed it *before* checking cache.
                    // To save CPU, I should NOT compute it if it's a hit.
                    // But I don't know if it's a hit until I check metadata.
                    // Checking metadata is cheap.
                    
                    // If I want to support "Trust-but-Verify" in the future, I should compute it for *new* files.
                    if !is_cached_hit {
                         entry.sparse_hash = compute_sparse_hash(&path, size).ok();
                    } else {
                        // Preserve old sparse hash if available?
                         if let Some(old) = cached_entry_opt.and_then(|e| e.sparse_hash) {
                             entry.sparse_hash = Some(old);
                         }
                    }

                    if let Some(chunks) = chunks_info {
                        entry.set_chunks(chunks)?;
                    }

                    Ok(ProcessedMessage {
                        path_str: name_str,
                        abs_path: path,
                        metadata_info: MetadataInfo {
                            size,
                            modified,
                            mode,
                        },
                        entry,
                        data_action,
                        is_cached_hit,
                    })
                })();

                match process_res {
                    Ok(msg) => {
                        let _ = tx.send(WorkerResult::Processed(msg));
                    }
                    Err(e) => {
                        let _ = tx.send(WorkerResult::Error(e.to_string()));
                    }
                }
            }
        }));
    }

    // 4. Writer Loop (Main Thread)
    drop(path_tx);
    drop(res_tx);

    let mut count = 0;
    let mut cache_hit_count = 0;
    let mut dedup_count = 0; // Content existed in blobs
    let mut total_raw_size = 0;
    let mut manifest = SnapshotManifest::default();
    let mut batch_counter = 0;

    while let Ok(msg) = res_rx.recv() {
        if !running.load(Ordering::SeqCst) {
            break;
        }

        match msg {
            WorkerResult::Error(e) => {
                eprintln!("{} Error: {}", "⚠️".yellow(), e);
            }
            WorkerResult::Processed(pm) => {
                total_raw_size += pm.metadata_info.size;
                
                if pm.is_cached_hit {
                    cache_hit_count += 1;
                }

                // Handle Data Writing
                match pm.data_action {
                    DataAction::Cached => {
                        // Nothing to write (Deduplicated completely)
                        pb.set_message(format!("Dedup: {}", pm.path_str));
                        dedup_count += 1;
                    }
                    DataAction::WriteFile(hash_bytes) => {
                        let hash_hex = hex::encode(&hash_bytes);
                        // Double check blobs (though worker checked it)
                         // Worker checked against shared map, but main thread writes.
                         // It's possible another thread added it.
                         // But for now trust worker's decision or check again?
                         // DashMap is concurrent.
                        if !written_blobs.contains_key(&hash_hex) {
                            pb.set_message(format!("Writing: {}", pm.path_str));
                            let blob_path = format!("blobs/{}", hash_hex);
                            let mut f = File::open(&pm.abs_path)?;
                            tar.append_file(&blob_path, &mut f)?;
                            written_blobs.insert(hash_hex, ());
                        } else {
                            pb.set_message(format!("Dedup: {}", pm.path_str));
                            dedup_count += 1;
                        }
                    }
                    DataAction::WriteChunks(chunks) => {
                        pb.set_message(format!("Chunking: {}", pm.path_str));
                        let mut f = File::open(&pm.abs_path)?;
                        let mut any_written = false;
                        for chunk in chunks {
                            let chunk_hex = hex::encode(chunk.hash);
                            if !written_blobs.contains_key(&chunk_hex) {
                                let blob_path = format!("blobs/{}", chunk_hex);
                                f.seek(SeekFrom::Start(chunk.offset))?;
                                let mut chunk_buf = vec![0u8; chunk.length as usize];
                                f.read_exact(&mut chunk_buf)?;

                                let mut header = tar::Header::new_gnu();
                                header.set_path(&blob_path)?;
                                header.set_size(chunk.length as u64);
                                header.set_mode(0o644);
                                header.set_cksum();
                                tar.append_data(&mut header, &blob_path, &chunk_buf[..])?;

                                written_blobs.insert(chunk_hex, ());
                                any_written = true;
                            }
                        }
                        if !any_written {
                             dedup_count += 1;
                        }
                    }
                }

                // Update Cache DB
                cache_db.insert(&pm.path_str, &pm.entry)?;

                // Update Manifest
                let chunk_hashes_hex: Option<Vec<String>> = pm
                    .entry
                    .get_chunks()
                    .ok()
                    .flatten()
                    .map(|v| v.iter().map(|c| hex::encode(c.hash)).collect());

                manifest.entries.push(ManifestEntry {
                    path: pm.path_str,
                    hash: hex::encode(pm.entry.hash.unwrap_or_default()),
                    size: pm.metadata_info.size,
                    modified: pm.metadata_info.modified,
                    mode: pm.metadata_info.mode,
                    chunks: chunk_hashes_hex,
                });

                count += 1;
                batch_counter += 1;

                if batch_counter >= BATCH_COMMIT_SIZE {
                    cache_db.commit_batch()?;
                    batch_counter = 0;
                }
            }
        }
    }

    let _ = scanner_handle.join();
    for h in worker_handles {
        let _ = h.join();
    }

    if !running.load(Ordering::SeqCst) {
        pb.finish_with_message("Interrupted!");
        if !no_cache {
            let _ = cache_db.sync_partial();
        }
        return Err(anyhow::anyhow!("Interrupted"));
    }

    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_path("manifest.json")?;
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_json.as_bytes())?;

    if !no_cache {
        println!("{} Running garbage collection...", "🧹".blue());
        match cache_db.garbage_collect(CACHE_RETENTION_SEC) {
            Ok(count) => {
                if count > 0 {
                    println!("{} GC: Removed {} expired entries", "🧹".blue(), count);
                }
            }
            Err(e) => eprintln!("{} GC Warning: {}", "⚠️".yellow(), e),
        }
        if let Err(e) = cache_db.commit() {
            eprintln!("{} Failed to save cache: {}", "⚠️".yellow(), e);
        }
    }

    pb.finish_with_message(format!(
        "Packed {} files ({} cache hits, {} deduped, {} blobs) using {} threads.",
        count,
        cache_hit_count,
        dedup_count,
        written_blobs.len(),
        num_threads
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

        let mut dest_file = File::create(&dest_path)?;
        let chunk_hashes = entry.chunks.unwrap_or_else(|| vec![entry.hash.clone()]);

        for chunk_hash in chunk_hashes {
            let blob_path = blobs_dir.join(&chunk_hash);
            if blob_path.exists() {
                let mut blob_file = File::open(&blob_path)?;
                std::io::copy(&mut blob_file, &mut dest_file)?;
            } else {
                pb.println(format!(
                    "{} Missing blob {} for {}",
                    "⚠️".yellow(),
                    chunk_hash,
                    entry.path
                ));
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(entry.mode);
            fs::set_permissions(&dest_path, permissions)?;
        }

        pb.inc(1);
    }

    pb.finish_with_message("Restoration Complete!");

    let _ = fs::remove_file(manifest_path);
    let _ = fs::remove_dir_all(blobs_dir);
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
                println!(
                    "{:<40} {:>8} bytes [Chunks: {}] [{}]",
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

    const CHUNK_THRESHOLD: u64 = 100 * 1024 * 1024;

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
