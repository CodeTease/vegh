mod cli;
mod core;
mod hash;
mod storage;

use anyhow::Result;
use clap::Parser;
use colored::*;
use std::path::PathBuf;
use std::time::Instant;
use chrono::Utc;

use cli::{Cli, Commands};
use core::{create_snap, restore_snap, list_snap, send_file, format_bytes};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let start_time = Instant::now();

    match cli.command {
        Commands::Snap {
            path,
            output,
            level,
            comment,
            include,
            exclude,
            no_cache,
        } => {
            let folder_name = path
                .file_name()
                .unwrap_or(std::ffi::OsStr::new("backup"))
                .to_string_lossy();
            let output_path = output.unwrap_or(PathBuf::from(format!("{}.vegh", folder_name)));

            println!(
                "{} Packing '{}' -> '{}'",
                "⚙️".cyan(),
                path.display(),
                output_path.display()
            );

            // Spawn blocking task for CPU-intensive compression work
            let task = tokio::task::spawn_blocking(move || {
                create_snap(
                    &path,
                    &output_path,
                    level,
                    comment,
                    include,
                    exclude,
                    no_cache,
                )
            });

            match task.await? {
                Ok((raw_size, compressed_size)) => {
                    let ratio = if compressed_size > 0 {
                        raw_size as f64 / compressed_size as f64
                    } else {
                        0.0
                    };
                    println!("{} Done! ({:.2?})", "✨".green(), start_time.elapsed());
                    println!(
                        "   Compression: {:.2}x ({} -> {})",
                        ratio,
                        format_bytes(raw_size),
                        format_bytes(compressed_size)
                    );
                }
                Err(e) => {
                    // Gracefully handle errors, including user interruptions
                    eprintln!("{} Operation failed: {}", "❌".red(), e);
                }
            }
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
            
            let task = tokio::task::spawn_blocking(move || crate::hash::check_integrity(&file));
            let (hash, meta) = task.await??;

            println!("{} Integrity OK!", "✅".green());
            println!("   BLAKE3: {}", hash.dimmed());
            if let Some(m) = meta {
                println!("\n{} Metadata:", "🏷️".blue());
                println!("   Author:  {}", m.author.cyan());

                if let Some(human_time) = m.timestamp_human {
                    println!("   Time:    {}", human_time.yellow());
                } else {
                    let dt =
                        chrono::DateTime::<Utc>::from_timestamp(m.timestamp, 0).unwrap_or_default();
                    println!("   Time:    {}", dt.to_rfc3339().yellow());
                }

                println!("   Vegh Ver: {}", m.tool_version.magenta());
                println!("   Format:   v{}", m.format_version.bold());
                if !m.comment.is_empty() {
                    println!("   Comment: {}", m.comment.italic());
                }
            } else {
                println!("\n{} No metadata (legacy/raw archive).", "⚠️".yellow());
            }
        }
        Commands::Send {
            file,
            url,
            force_chunk,
            auth,
        } => {
            send_file(&file, &url, force_chunk, auth).await?;
            println!("{} Sent! ({:.2?})", "🚀".green(), start_time.elapsed());
        }
    }
    Ok(())
}
