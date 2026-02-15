use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// 🥬 Vegh - The Snapshot Tool
#[derive(Parser)]
#[command(name = "vg")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Create a snapshot (.vegh)
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
        // Flag to ignore existing cache and force a full rebuild
        #[arg(long)]
        no_cache: bool,
    },
    /// Restore a snapshot
    Restore {
        file: PathBuf,
        #[arg(default_value = ".")]
        out_dir: PathBuf,
    },
    /// List files
    List { file: PathBuf },
    /// Verify integrity (Uses Blake3)
    Check { file: PathBuf },
    /// Send to server
    Send {
        file: PathBuf,
        url: String,
        #[arg(long)]
        force_chunk: bool,
        #[arg(long)]
        auth: Option<String>,
    },
}
