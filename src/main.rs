use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use futures::stream::{self, StreamExt};
use ignore::{WalkBuilder, overrides::OverrideBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use serde::{Serialize, Deserialize};
use chrono::Utc;

// Files that should ALWAYS be included
const PRESERVED_FILES: &[&str] = &[".veghignore", ".gitignore"];
// [FIX] Đồng bộ Format Version với PyVegh.
// Bump số này lên CHỈ KHI cấu trúc file .snap thay đổi.
const SNAPSHOT_FORMAT_VERSION: &str = "1";

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

        /// Include files (force add, bypass ignore)
        #[arg(short = 'i', long)]
        include: Vec<String>,

        /// Exclude files (force ignore)
        #[arg(short = 'e', long)]
        exclude: Vec<String>,
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
    /// ✅ Verify integrity
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
    // [FIX] Đánh dấu Option để tương thích ngược với các bản build cũ hoặc từ PyVegh (nếu thiếu)
    #[serde(default)] 
    timestamp_human: Option<String>, 
    comment: String,
    tool_version: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let start_time = Instant::now();

    match cli.command {
        Commands::Snap { path, output, level, comment, include, exclude } => {
            let folder_name = path.file_name().unwrap_or(std::ffi::OsStr::new("backup")).to_string_lossy();
            let output_path = output.unwrap_or(PathBuf::from(format!("{}.snap", folder_name)));
            
            println!("{} Packing '{}' -> '{}'", "⚙️".cyan(), path.display(), output_path.display());
            
            let task = tokio::task::spawn_blocking(move || 
                create_snap(&path, &output_path, level, comment, include, exclude)
            );
            task.await??;
            println!("{} Done! ({:.2?})", "✨".green(), start_time.elapsed());
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
            println!("   SHA256: {}", hash.dimmed());
            if let Some(m) = meta {
                println!("\n{} Metadata:", "🏷️".blue());
                println!("   Author:  {}", m.author.cyan());
                
                // [FIX] Handle optional timestamp_human
                if let Some(human_time) = m.timestamp_human {
                    println!("   Time:    {}", human_time.yellow());
                } else {
                     // Fallback convert từ timestamp
                    let dt = chrono::DateTime::<Utc>::from_timestamp(m.timestamp, 0).unwrap_or_default();
                    println!("   Time:    {}", dt.to_rfc3339().yellow());
                }

                println!("   Format:  {}", m.tool_version.magenta()); // Hiển thị Format Version
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

fn create_snap(
    source: &Path, 
    output: &Path, 
    level: i32, 
    comment: Option<String>,
    include: Vec<String>,
    exclude: Vec<String>
) -> Result<()> {
    let file = File::create(output).context("Output file creation failed")?;
    let output_abs = fs::canonicalize(output).unwrap_or(output.to_path_buf());

    let meta = VeghMetadata {
        author: "CodeTease".to_string(),
        timestamp: Utc::now().timestamp(),
        timestamp_human: Some(Utc::now().to_rfc3339()),
        comment: comment.unwrap_or_default(),
        // [FIX] Sử dụng Format Version cố định thay vì Cargo Pkg Version
        tool_version: SNAPSHOT_FORMAT_VERSION.to_string(),
    };
    let meta_json = serde_json::to_string_pretty(&meta)?;

    let encoder = zstd::stream::write::Encoder::new(file, level)?;
    let mut tar = tar::Builder::new(encoder);

    // Meta (Hidden)
    let mut header = tar::Header::new_gnu();
    header.set_path(".vegh.json")?;
    header.set_size(meta_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, ".vegh.json", meta_json.as_bytes())?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg}").unwrap());
    let mut count = 0;

    // Preserved Files (VIPs)
    for &filename in PRESERVED_FILES {
        let p = source.join(filename);
        if p.exists() && p.is_file() {
            let mut f = File::open(&p)?;
            tar.append_file(filename, &mut f)?;
            count += 1;
        }
    }

    // Override Logic (Silent)
    let mut override_builder = OverrideBuilder::new(source);
    
    for pattern in include {
        let _ = override_builder.add(&format!("!{}", pattern));
    }
    for pattern in exclude {
        let _ = override_builder.add(&pattern);
    }

    let overrides = override_builder.build().context("Failed to build override rules")?;

    let mut builder = WalkBuilder::new(source);
    for &f in PRESERVED_FILES { builder.add_custom_ignore_filename(f); }
    
    builder
        .hidden(true)
        .git_ignore(true)
        .overrides(overrides); 
    
    for result in builder.build() {
        if let Ok(entry) = result {
            let path = entry.path();
            if path.is_file() {
                if let Ok(abs) = fs::canonicalize(path) { if abs == output_abs { continue; } }
                let name = path.strip_prefix(source).unwrap_or(path);
                if PRESERVED_FILES.contains(&name.to_string_lossy().as_ref()) { continue; }
                
                pb.set_message(format!("Packing: {}", name.display()));
                tar.append_path_with_name(path, name)?;
                count += 1;
            }
        }
    }
    pb.finish_with_message(format!("Packed {} files.", count));

    let zstd_encoder = tar.into_inner()?;
    zstd_encoder.finish()?;
    
    Ok(())
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

fn check_integrity(input: &Path) -> Result<(String, Option<VeghMetadata>)> {
    let mut f = File::open(input)?;
    let mut sha = Sha256::new();
    std::io::copy(&mut f, &mut sha)?;
    let hash = hex::encode(sha.finalize());

    // Try to read meta
    let file = File::open(input)?;
    let meta = if let Ok(d) = zstd::stream::read::Decoder::new(file) {
        let mut ar = tar::Archive::new(d);
        ar.entries()
            .ok()
            .and_then(|mut entries| {
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

// Chunked Upload Logic
async fn send_file(path: &Path, url: &str, force_chunk: bool, auth_token: Option<String>) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("File not found: {}", path.display());
    }

    let metadata = path.metadata()?;
    let file_size = metadata.len();
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    println!("{} Target: {}", "🌐".cyan(), url);
    println!("{} File: {} ({:.2} MB)", "📄".cyan(), filename, file_size as f64 / 1024.0 / 1024.0);
    
    if let Some(_) = &auth_token {
        println!("{} Authentication: Enabled", "🔒".green());
    }

    const CHUNK_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
    
    if file_size < CHUNK_THRESHOLD && !force_chunk {
        println!("{} Mode: Direct Upload", "⚡".yellow());
        send_direct(path, url, file_size, auth_token).await
    } else {
        println!("{} Mode: Concurrent Chunked Upload", "📦".yellow());
        send_chunked(path, url, file_size, &filename, auth_token).await
    }
}

async fn send_direct(path: &Path, url: &str, file_size: u64, auth_token: Option<String>) -> Result<()> {
    let client = Client::new();
    let mut file = AsyncFile::open(path).await?;
    let mut buffer = Vec::with_capacity(file_size as usize);
    file.read_to_end(&mut buffer).await?;

    let pb = ProgressBar::new(file_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    pb.set_message("Uploading...");
    
    let mut request = client.post(url);

    if let Some(token) = auth_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let response = request.body(buffer).send().await?;

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

async fn send_chunked(path: &Path, url: &str, file_size: u64, filename: &str, auth_token: Option<String>) -> Result<()> {
    const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB
    let chunk_size_u64 = CHUNK_SIZE as u64;
    let total_chunks = (file_size + chunk_size_u64 - 1) / chunk_size_u64;
    
    let pb = ProgressBar::new(total_chunks);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} chunks ({eta})")
        .unwrap()
        .progress_chars("█▒░"));

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
                    .header("X-Total-Chunks", total_chunks.to_string());

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
        .buffer_unordered(4); // Process 4 chunks concurrently

    let results: Vec<Result<()>> = stream.collect().await;
    for res in results {
        if let Err(e) = res { anyhow::bail!("Upload incomplete. Error: {}", e); }
    }

    pb.finish_with_message("All chunks sent successfully!");
    Ok(())
}