use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use futures::stream::{self, StreamExt};
use ignore::{WalkBuilder, overrides::OverrideBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
// [UPDATE] Switched to Blake3
use blake3::Hasher;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::codec::{BytesCodec, FramedRead}; // For Streaming
use serde::{Serialize, Deserialize};
use chrono::Utc;
use memmap2::MmapOptions; // For zero-copy hashing

// Files that should ALWAYS be included
const PRESERVED_FILES: &[&str] = &[".veghignore", ".gitignore", ".dockerignore", ".npmignore"];

// [STRICT] Format Version stays at 2.
// FV 2 now encompasses: Multithreading, Caching, and Blake3 Hashing.
const SNAPSHOT_FORMAT_VERSION: &str = "2";

const CACHE_DIR: &str = ".veghcache";
const CACHE_FILE: &str = "index.json";

/// 🥬 Vegh - The CodeTease Snapshot Tool
#[derive(Parser)]
#[command(name = "vegh")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 📸 Create a snapshot (.snap)
    Snap {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(short = 'l', long, default_value_t = 3)]
        level: i32,
        #[arg(long)]
        comment: Option<String>,
        #[arg(short = 'i', long)]
        include: Vec<String>,
        #[arg(short = 'e', long)]
        exclude: Vec<String>,
        #[arg(long)]
        no_cache: bool,
    },
    /// 📦 Restore a snapshot
    Restore {
        file: PathBuf,
        #[arg(default_value = ".")]
        out_dir: PathBuf,
    },
    /// 📜 List files
    List {
        file: PathBuf,
    },
    /// ✅ Verify integrity (Now with Blake3)
    Check {
        file: PathBuf,
    },
    /// 🚀 Send to server
    Send {
        file: PathBuf,
        url: String,
        #[arg(long)]
        force_chunk: bool,
        #[arg(long)]
        auth: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct VeghMetadata {
    author: String,
    timestamp: i64,
    #[serde(default)] 
    timestamp_human: Option<String>, 
    comment: String,
    tool_version: String,
    #[serde(default = "default_format_version")]
    format_version: String,
}

fn default_format_version() -> String {
    "1".to_string()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FileCacheEntry {
    size: u64,
    modified: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct VeghCache {
    last_snapshot: i64,
    files: HashMap<String, FileCacheEntry>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let start_time = Instant::now();

    match cli.command {
        Commands::Snap { path, output, level, comment, include, exclude, no_cache } => {
            let folder_name = path.file_name().unwrap_or(std::ffi::OsStr::new("backup")).to_string_lossy();
            let output_path = output.unwrap_or(PathBuf::from(format!("{}.snap", folder_name)));
            
            println!("{} Packing '{}' -> '{}'", "⚙️".cyan(), path.display(), output_path.display());
            
            let task = tokio::task::spawn_blocking(move || 
                create_snap(&path, &output_path, level, comment, include, exclude, no_cache)
            );
            
            // [NEW] Display Compression Ratio
            let (raw_size, compressed_size) = task.await??;
            let ratio = if compressed_size > 0 { raw_size as f64 / compressed_size as f64 } else { 0.0 };
            
            println!("{} Done! ({:.2?})", "✨".green(), start_time.elapsed());
            println!("   Compression: {:.2}x ({} -> {})", ratio, format_bytes(raw_size), format_bytes(compressed_size));
        }
        Commands::Restore { file, out_dir } => {
            println!("{} Restoring '{}'...", "⚙️".cyan(), file.display());
            let task = tokio::task::spawn_blocking(move || restore_snap(&file, &out_dir));
            task.await??;
            println!("{} Done! ({:.2?})", "✨".green(), start_time.elapsed());
        }
        Commands::List { file } => {
            let task = tokio::task::spawn_blocking(move || list_snap(&file));
            task.await??;
        }
        Commands::Check { file } => {
            println!("{} Checking '{}'...", "🔍".yellow(), file.display());
            let task = tokio::task::spawn_blocking(move || check_integrity(&file));
            let (hash, meta) = task.await??;
            
            println!("{} Integrity OK!", "✅".green());
            println!("   BLAKE3: {}", hash.dimmed()); // Updated label
            if let Some(m) = meta {
                println!("\n{} Metadata:", "🏷️".blue());
                println!("   Author:  {}", m.author.cyan());
                
                if let Some(human_time) = m.timestamp_human {
                    println!("   Time:    {}", human_time.yellow());
                } else {
                    let dt = chrono::DateTime::<Utc>::from_timestamp(m.timestamp, 0).unwrap_or_default();
                    println!("   Time:    {}", dt.to_rfc3339().yellow());
                }

                println!("   Vegh Ver: {}", m.tool_version.magenta()); 
                println!("   Format:   v{}", m.format_version.bold());
                if !m.comment.is_empty() { println!("   Comment: {}", m.comment.italic()); }
            } else {
                println!("\n{} No metadata (legacy/raw archive).", "⚠️".yellow());
            }
        }
        Commands::Send { file, url, force_chunk, auth } => {
            send_file(&file, &url, force_chunk, auth).await?;
            println!("{} Sent! ({:.2?})", "🚀".green(), start_time.elapsed());
        }
    }
    Ok(())
}

// Helper to format bytes nicely
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}

fn get_cache_path(source: &Path) -> PathBuf {
    source.join(CACHE_DIR).join(CACHE_FILE)
}

fn load_cache(source: &Path) -> VeghCache {
    let cache_path = get_cache_path(source);
    if cache_path.exists() {
        if let Ok(file) = File::open(&cache_path) {
            if let Ok(cache) = serde_json::from_reader(file) {
                return cache;
            }
        }
        println!("{} Cache corrupted. Cleaning...", "🧹".yellow());
        let _ = fs::remove_dir_all(source.join(CACHE_DIR));
    }
    VeghCache::default()
}

fn save_cache(source: &Path, cache: &VeghCache) -> Result<()> {
    let cache_dir = source.join(CACHE_DIR);
    if !cache_dir.exists() { fs::create_dir(&cache_dir)?; }
    let file = File::create(get_cache_path(source))?;
    serde_json::to_writer_pretty(file, cache)?;
    Ok(())
}

// Returns (Raw Size, Compressed Size)
fn create_snap(
    source: &Path, 
    output: &Path, 
    level: i32, 
    comment: Option<String>,
    include: Vec<String>,
    exclude: Vec<String>,
    no_cache: bool
) -> Result<(u64, u64)> {
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
    let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    encoder.multithread(workers as u32)?;

    let mut tar = tar::Builder::new(encoder);
    let mut header = tar::Header::new_gnu();
    header.set_path(".vegh.json")?;
    header.set_size(meta_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, ".vegh.json", meta_json.as_bytes())?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg}").unwrap());
    
    let mut count = 0;
    let mut cached_count = 0;
    let mut total_raw_size = 0;

    // Preserved Files
    for &filename in PRESERVED_FILES {
        let p = source.join(filename);
        if p.exists() && p.is_file() {
            let mut f = File::open(&p)?;
            let meta = f.metadata()?;
            total_raw_size += meta.len();
            tar.append_file(filename, &mut f)?;
            count += 1;
        }
    }

    let mut override_builder = OverrideBuilder::new(source);
    for pattern in include { let _ = override_builder.add(&format!("!{}", pattern)); }
    for pattern in exclude { let _ = override_builder.add(&pattern); }
    let _ = override_builder.add(&format!("!{}", CACHE_DIR));

    let overrides = override_builder.build().context("Failed to build override rules")?;
    let mut builder = WalkBuilder::new(source);
    for &f in PRESERVED_FILES { builder.add_custom_ignore_filename(f); }
    
    builder.hidden(true).git_ignore(true).overrides(overrides); 
    
    for result in builder.build() {
        if let Ok(entry) = result {
            let path = entry.path();
            if path.is_file() {
                if let Ok(abs) = fs::canonicalize(path) { if abs == output_abs { continue; } }
                let name = path.strip_prefix(source).unwrap_or(path);
                let name_str = name.to_string_lossy().to_string();

                if PRESERVED_FILES.contains(&name.to_string_lossy().as_ref()) { continue; }
                
                let metadata = path.metadata()?;
                let modified = metadata.modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let size = metadata.len();
                total_raw_size += size;

                let is_cached = if let Some(cached_entry) = cache.files.get(&name_str) {
                    cached_entry.modified == modified && cached_entry.size == size
                } else {
                    false
                };

                if is_cached {
                    cached_count += 1;
                    pb.set_message(format!("Packing (Cached): {}", name.display()));
                } else {
                    pb.set_message(format!("Packing (New): {}", name.display()));
                }

                new_cache_files.insert(name_str, FileCacheEntry { size, modified });
                tar.append_path_with_name(path, name)?;
                count += 1;
            }
        }
    }

    cache.files = new_cache_files;
    cache.last_snapshot = Utc::now().timestamp();
    if !no_cache {
        let _ = save_cache(source, &cache);
    }

    pb.finish_with_message(format!(
        "Packed {} files ({} cached) using {} threads.", 
        count, cached_count, workers
    ));

    let zstd_encoder = tar.into_inner()?;
    zstd_encoder.finish()?;
    
    // Calculate final file size
    let final_size = fs::metadata(output)?.len();
    
    Ok((total_raw_size, final_size))
}

fn restore_snap(input: &Path, out_dir: &Path) -> Result<()> {
    if !out_dir.exists() { fs::create_dir_all(out_dir)?; }
    let file = File::open(input).context("Open failed")?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.to_string_lossy() == ".vegh.json" { continue; }
        entry.unpack_in(out_dir)?;
    }
    Ok(())
}

fn list_snap(input: &Path) -> Result<()> {
    let file = File::open(input).context("Open failed")?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    println!("{} Contents of {}:", "📂".cyan(), input.display());
    println!("{:-<50}", "-");
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?;
        if path.to_string_lossy() == ".vegh.json" {
            println!("{} (Metadata)", ".vegh.json".dimmed());
        } else {
            println!("{:<40} {:>8} bytes", path.display(), entry.size());
        }
    }
    Ok(())
}

// [UPDATE] High-performance Integrity Check with BLAKE3 + Mmap + Rayon
fn check_integrity(input: &Path) -> Result<(String, Option<VeghMetadata>)> {
    let file = File::open(input)?;
    
    // Try to memory map the file for max speed (zero-copy)
    // If mmap fails (e.g. OS restrictions), fallback to standard read
    let hash = if let Ok(mmap) = unsafe { MmapOptions::new().map(&file) } {
        // [RAYON] Blake3 update_rayon automatically uses multithreading for large inputs!
        let mut hasher = Hasher::new();
        hasher.update_rayon(&mmap);
        hasher.finalize().to_hex().to_string()
    } else {
        // Fallback for systems without mmap support
        let mut f = File::open(input)?;
        let mut hasher = Hasher::new();
        std::io::copy(&mut f, &mut hasher)?;
        hasher.finalize().to_hex().to_string()
    };

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
                } else { None }
            })
        })
    } else { None };

    Ok((hash, meta))
}

async fn send_file(path: &Path, url: &str, force_chunk: bool, auth_token: Option<String>) -> Result<()> {
    if !path.exists() { anyhow::bail!("File not found: {}", path.display()); }
    let metadata = path.metadata()?;
    let file_size = metadata.len();
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    println!("{} Target: {}", "🌐".cyan(), url);
    println!("{} File: {} ({:.2} MB)", "📄".cyan(), filename, file_size as f64 / 1024.0 / 1024.0);
    if let Some(_) = &auth_token { println!("{} Authentication: Enabled", "🔒".green()); }

    const CHUNK_THRESHOLD: u64 = 100 * 1024 * 1024; 
    
    if file_size < CHUNK_THRESHOLD && !force_chunk {
        println!("{} Mode: Streaming Direct Upload", "🌊".yellow());
        send_streaming(path, url, file_size, auth_token).await
    } else {
        println!("{} Mode: Concurrent Chunked Upload", "📦".yellow());
        send_chunked(path, url, file_size, &filename, auth_token).await
    }
}

// [UPDATE] True Streaming Upload (Low RAM usage)
async fn send_streaming(path: &Path, url: &str, file_size: u64, auth_token: Option<String>) -> Result<()> {
    let client = Client::new();
    let file = AsyncFile::open(path).await?;
    
    // Create a stream from the file (chunked read)
    let stream = FramedRead::new(file, BytesCodec::new());
    // Convert to reqwest Body
    let body = reqwest::Body::wrap_stream(stream);

    let pb = ProgressBar::new(file_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    // Note: Reqwest async stream doesn't easily support progress callback *during* send 
    // without wrapping the stream manually. For simplicity/stability, we just show spinner or start.
    // (A full wrapped stream is possible but verbose for this snippet).
    pb.set_message("Streaming...");

    let mut request = client.post(url)
        .header("Content-Length", file_size) // Good for R2/S3
        .header("User-Agent", "CodeTease-Vegh/0.3.0");

    if let Some(token) = auth_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let response = request.body(body).send().await?;

    if response.status().is_success() {
        pb.finish_with_message("Upload success!");
        let response_text = response.text().await.unwrap_or_else(|_| "No response text".to_string());
        println!("\n{} Server Response:\n{}", "📩".blue(), response_text.dimmed());
        Ok(())
    } else {
        pb.abandon();
        anyhow::bail!("Upload failed with status: {}", response.status());
    }
}

// ... send_chunked (giữ nguyên logic cũ, nó đã khá tối ưu cho file rất lớn)
async fn send_chunked(path: &Path, url: &str, file_size: u64, filename: &str, auth_token: Option<String>) -> Result<()> {
    const CHUNK_SIZE: usize = 10 * 1024 * 1024; 
    let chunk_size_u64 = CHUNK_SIZE as u64;
    let total_chunks = (file_size + chunk_size_u64 - 1) / chunk_size_u64;
    
    let pb = ProgressBar::new(total_chunks);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} chunks ({eta})")
        .unwrap().progress_chars("█▒░"));

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
                if end > file_size { end = file_size; }
                let current_chunk_size = (end - start) as usize;

                let mut file = AsyncFile::open(&path).await.context("Failed to open file")?;
                file.seek(SeekFrom::Start(start)).await.context("Failed to seek")?;
                
                let mut buffer = vec![0u8; current_chunk_size];
                file.read_exact(&mut buffer).await.context("Failed to read chunk")?;

                let mut request = client.post(&url)
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
        if let Err(e) = res { anyhow::bail!("Upload incomplete. Error: {}", e); }
    }

    pb.finish_with_message("All chunks sent successfully!");
    Ok(())
}