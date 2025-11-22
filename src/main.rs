use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use futures::stream::{self, StreamExt};
use ignore::WalkBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, SeekFrom}; // Trait 'Seek' removed to satisfy Cargo
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt}; // AsyncSeekExt is needed for seek()

/// 🥬 Vegh - The CodeTease Snapshot Tool
/// "Tight packing, swift unpacking, no nonsense."
#[derive(Parser)]
#[command(name = "vegh")]
#[command(about = "The ultimate snapshot tool by CodeTease", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 📸 Create a snapshot (.snap) from a directory
    Snap {
        /// Path to the source directory
        #[arg(default_value = ".")]
        path: PathBuf,

        /// Output file name (default: <folder_name>.snap)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// 📦 Restore a snapshot (.snap)
    Restore {
        /// The .snap file to restore
        file: PathBuf,

        /// Destination directory (optional, default: current directory)
        /// Example: vegh restore my-project.snap ./output-folder
        #[arg(default_value = ".")]
        out_dir: PathBuf,
    },
    /// ✅ Verify the integrity of a .snap file
    Check {
        /// The .snap file to verify
        file: PathBuf,
    },
    /// 🚀 Send a snapshot to a remote server
    Send {
        /// The file to send
        file: PathBuf,

        /// The target URL to receive the file
        url: String,

        /// Force chunking regardless of file size
        #[arg(long)]
        force_chunk: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let start_time = Instant::now();

    match cli.command {
        Commands::Snap { path, output } => {
            // Determine output filename
            let folder_name = path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("backup"))
                .to_string_lossy()
                .to_string();
            let output_path = output.unwrap_or_else(|| PathBuf::from(format!("{}.snap", folder_name)));

            println!("{} Packing '{}' into '{}'...", "⚙️".cyan(), path.display(), output_path.display());

            // Run blocking IO/CPU tasks in a separate thread
            let task = tokio::task::spawn_blocking(move || create_snap(&path, &output_path));
            task.await??;

            println!("{} Done! Took {:.2?}", "✨".green(), start_time.elapsed());
        }
        Commands::Restore { file, out_dir } => {
            println!("{} Restoring '{}' to '{}'...", "⚙️".cyan(), file.display(), out_dir.display());

            let task = tokio::task::spawn_blocking(move || restore_snap(&file, &out_dir));
            task.await??;

            println!("{} Done! Took {:.2?}", "✨".green(), start_time.elapsed());
        }
        Commands::Check { file } => {
            println!("{} Inspecting '{}'...", "🔍".yellow(), file.display());
            
            let task = tokio::task::spawn_blocking(move || check_integrity(&file));
            let hash = task.await??;

            println!("{} Integrity verified! SHA256: {}", "✅".green(), hash);
        }
        Commands::Send { file, url, force_chunk } => {
            send_file(&file, &url, force_chunk).await?;
            println!("{} Transfer complete! Took {:.2?}", "🚀".green(), start_time.elapsed());
        }
    }

    Ok(())
}

// --- CORE LOGIC ---

/// Snapshot creation logic
fn create_snap(source: &Path, output: &Path) -> Result<()> {
    let file = File::create(output).context("Failed to create output file")?;
    
    // Zstd Encoder: Default compression level is 3 (Balanced).
    let encoder = zstd::stream::write::Encoder::new(file, 3)?;
    
    // Tar Builder wraps Zstd Encoder
    let mut tar = tar::Builder::new(encoder);

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg}").unwrap());

    // --- FIX: Initialize count BEFORE processing .veghignore ---
    let mut count = 0;

    // Explicitly add .veghignore first
    let ignore_filename = ".veghignore";
    let ignore_path = source.join(ignore_filename);
    
    if ignore_path.exists() && ignore_path.is_file() {
        pb.set_message(format!("Preserving config: {}", ignore_filename));
        let mut f = File::open(&ignore_path).context("Failed to open .veghignore")?;
        tar.append_file(ignore_filename, &mut f)
            .context("Failed to add .veghignore to archive")?;
        
        // --- FIX: Increment count for the config file ---
        count += 1;
    }

    // Configure walker to respect .veghignore rules for OTHER files
    let mut builder = WalkBuilder::new(source);
    builder.add_custom_ignore_filename(ignore_filename);
    builder.hidden(true); // Ignore hidden files (dotfiles) by default
    builder.git_ignore(true); 

    let walker = builder.build();

    for result in walker {
        match result {
            Ok(entry) => {
                let path = entry.path();
                if path.is_file() {
                    let name = path.strip_prefix(source).unwrap_or(path);
                    let name_str = name.to_string_lossy();

                    // Avoid adding .veghignore twice if the walker somehow picks it up
                    if name_str == ignore_filename {
                        continue;
                    }

                    pb.set_message(format!("Compressing: {}", name.display()));
                    tar.append_path_with_name(path, name)
                        .with_context(|| format!("Failed to add file: {}", path.display()))?;
                    count += 1;
                }
            }
            Err(err) => eprintln!("{} Error reading file: {}", "⚠️".red(), err),
        }
    }

    pb.finish_with_message(format!("Compressed {} files.", count));
    
    let encoder = tar.into_inner()?;
    encoder.finish()?;

    Ok(())
}

/// Snapshot restoration logic
fn restore_snap(input: &Path, out_dir: &Path) -> Result<()> {
    let file = File::open(input).context("Failed to find .snap file")?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(out_dir).context("Failed to unpack archive")?;
    Ok(())
}

/// Integrity check logic
fn check_integrity(input: &Path) -> Result<String> {
    let mut file = File::open(input).context("Failed to open file for checking")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];

    let pb = ProgressBar::new_spinner();
    pb.set_message("Calculating checksum...");

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 { break; }
        hasher.update(&buffer[..count]);
    }

    let result = hasher.finalize();
    pb.finish_and_clear();
    Ok(hex::encode(result))
}

// --- NETWORK LOGIC ---

const CHUNK_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
const CHUNK_SIZE: usize = 10 * 1024 * 1024;     // 10MB per chunk

async fn send_file(path: &Path, url: &str, force_chunk: bool) -> Result<()> {
    if !path.exists() {
        anyhow::bail!("File not found: {}", path.display());
    }

    let metadata = path.metadata()?;
    let file_size = metadata.len();
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    println!("{} Target: {}", "🌐".cyan(), url);
    println!("{} File: {} ({:.2} MB)", "📄".cyan(), filename, file_size as f64 / 1024.0 / 1024.0);

    if file_size < CHUNK_THRESHOLD && !force_chunk {
        println!("{} Mode: Direct Upload", "⚡".yellow());
        send_direct(path, url, file_size).await
    } else {
        println!("{} Mode: Concurrent Chunked Upload", "📦".yellow());
        send_chunked(path, url, file_size, &filename).await
    }
}

async fn send_direct(path: &Path, url: &str, file_size: u64) -> Result<()> {
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
    
    let response = client.post(url)
        .body(buffer)
        .send()
        .await?;

    if response.status().is_success() {
        pb.finish_with_message("Upload success!");
        
        // Print Response Body
        let response_text = response.text().await.unwrap_or_else(|_| "No response text".to_string());
        println!("\n{} Server Response:\n{}", "📩".blue(), response_text.dimmed());
        
        Ok(())
    } else {
        pb.abandon();
        anyhow::bail!("Upload failed with status: {}", response.status());
    }
}

async fn send_chunked(path: &Path, url: &str, file_size: u64, filename: &str) -> Result<()> {
    let chunk_size = CHUNK_SIZE as u64;
    let total_chunks = (file_size + chunk_size - 1) / chunk_size;
    
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

            async move {
                let start = i * chunk_size;
                let mut end = start + chunk_size;
                if end > file_size {
                    end = file_size;
                }
                let current_chunk_size = (end - start) as usize;

                let mut file = AsyncFile::open(&path).await.context("Failed to open file")?;
                file.seek(SeekFrom::Start(start)).await.context("Failed to seek")?;
                
                let mut buffer = vec![0u8; current_chunk_size];
                file.read_exact(&mut buffer).await.context("Failed to read chunk")?;

                let response = client.post(&url)
                    .header("X-File-Name", &filename)
                    .header("X-Chunk-Index", i.to_string())
                    .header("X-Total-Chunks", total_chunks.to_string())
                    .body(buffer)
                    .send()
                    .await;

                match response {
                    Ok(res) => {
                        if res.status().is_success() {
                            pb.inc(1);
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!("Chunk {} failed: {}", i, res.status()))
                        }
                    }
                    Err(e) => Err(anyhow::anyhow!("Network error on chunk {}: {}", i, e)),
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