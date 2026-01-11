use anyhow::{Context, Result};
use redb::{Database, TableDefinition, WriteTransaction, ReadableTable, ReadableDatabase};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use colored::Colorize;
use std::fs::{self, File};
use std::path::{Path};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Cursor;

// Cache Configuration
pub const CACHE_DIR: &str = ".veghcache";
const CACHE_DB_FILE: &str = "cache.redb";
const JSON_CACHE_FILE: &str = "index.json";

// Redb Tables - v3 Schema (Bumped for compression/parallel changes)
const TABLE_DATA_V3_A: TableDefinition<&str, &[u8]> = TableDefinition::new("data_v3_A");
const TABLE_DATA_V3_B: TableDefinition<&str, &[u8]> = TableDefinition::new("data_v3_B");
const TABLE_INODES_V3_A: TableDefinition<u64, &str> = TableDefinition::new("inodes_v3_A");
const TABLE_INODES_V3_B: TableDefinition<u64, &str> = TableDefinition::new("inodes_v3_B");
const TABLE_META: TableDefinition<&str, &str> = TableDefinition::new("meta");

// Cache Entry Structure
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct FileCacheEntry {
    pub size: u64,
    pub modified: u64,
    
    #[serde(default)]
    pub ctime_sec: i64,
    #[serde(default)]
    pub ctime_nsec: u32,
    #[serde(default)]
    pub device_id: u64,
    #[serde(default)]
    pub inode: u64,
    
    #[serde(default)]
    pub last_seen: u64,

    pub hash: Option<[u8; 32]>,
    
    // [FV3] Compressed Chunks
    #[serde(default)]
    pub chunks_compressed: Option<Vec<u8>>,
    
    #[serde(default)]
    pub sparse_hash: Option<[u8; 32]>,
}

impl FileCacheEntry {
    pub fn set_chunks(&mut self, chunks: Vec<[u8; 32]>) -> Result<()> {
        // Flatten [u8; 32] to Vec<u8>
        let flat: Vec<u8> = chunks.iter().flat_map(|c| c.iter()).copied().collect();
        // Compress
        let compressed = zstd::stream::encode_all(Cursor::new(flat), 3)?; // Level 3 is fast
        self.chunks_compressed = Some(compressed);
        Ok(())
    }

    pub fn get_chunks(&self) -> Result<Option<Vec<[u8; 32]>>> {
        if let Some(compressed) = &self.chunks_compressed {
            let decompressed = zstd::stream::decode_all(Cursor::new(compressed))?;
            // Reconstruct [u8; 32] chunks
            if decompressed.len() % 32 != 0 {
                return Ok(None); // Corrupt?
            }
            let count = decompressed.len() / 32;
            let mut chunks = Vec::with_capacity(count);
            for i in 0..count {
                let slice = &decompressed[i*32..(i+1)*32];
                chunks.push(slice.try_into()?);
            }
            Ok(Some(chunks))
        } else {
            Ok(None)
        }
    }
}

// FV3 Manifest Structures
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestEntry {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub modified: u64,
    #[serde(default)]
    pub mode: u32,
    #[serde(default)]
    pub chunks: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SnapshotManifest {
    pub entries: Vec<ManifestEntry>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct LegacyVeghCache {
    pub last_snapshot: i64,
    pub files: HashMap<String, LegacyFileCacheEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LegacyFileCacheEntry {
    pub size: u64,
    pub modified: u64,
    pub inode: u64,
    pub hash: Option<String>,
    pub chunks: Option<Vec<String>>,
    pub sparse_hash: Option<String>,
}

// Reader Handle
#[derive(Clone)]
pub struct CacheReader {
    db: Arc<Database>,
    active_slot: String,
}

impl CacheReader {
    pub fn get(&self, path: &str) -> Result<Option<FileCacheEntry>> {
        let txn = self.db.begin_read()?;
        let bytes_opt: Option<Vec<u8>> = if self.active_slot == "A" {
            if let Ok(table) = txn.open_table(TABLE_DATA_V3_A) {
                table.get(path)?.map(|v| v.value().to_vec())
            } else { None }
        } else {
             if let Ok(table) = txn.open_table(TABLE_DATA_V3_B) {
                table.get(path)?.map(|v| v.value().to_vec())
            } else { None }
        };

        if let Some(bytes) = bytes_opt {
            let entry: FileCacheEntry = bincode::deserialize(&bytes)?;
            return Ok(Some(entry));
        }
        Ok(None)
    }

    pub fn get_path_by_inode(&self, inode: u64) -> Result<Option<String>> {
        if inode == 0 { return Ok(None); }
        let txn = self.db.begin_read()?;
        let path_opt = if self.active_slot == "A" {
            if let Ok(table) = txn.open_table(TABLE_INODES_V3_A) {
                table.get(inode)?.map(|v| v.value().to_string())
            } else { None }
        } else {
            if let Ok(table) = txn.open_table(TABLE_INODES_V3_B) {
                table.get(inode)?.map(|v| v.value().to_string())
            } else { None }
        };
        Ok(path_opt)
    }
}

pub struct CacheDB {
    db: Arc<Database>,
    active_slot: String,
    txn: Option<WriteTransaction>,
}

impl CacheDB {
    pub fn open(source: &Path) -> Result<Self> {
        let cache_dir = source.join(CACHE_DIR);
        if !cache_dir.exists() {
            fs::create_dir(&cache_dir).context("Failed to create cache dir")?;
            let gitignore_path = cache_dir.join(".gitignore");
            if !gitignore_path.exists() {
                 let content = "# Generated by Vegh\n*\n";
                 let _ = fs::write(gitignore_path, content);
            }
        }

        let db_path = cache_dir.join(CACHE_DB_FILE);
        let db = Database::create(&db_path)?;
        let db = Arc::new(db);

        let active_slot = {
            let read_txn = db.begin_read()?;
            if let Ok(table) = read_txn.open_table(TABLE_META) {
                table.get("active_slot")?
                    .map(|v| v.value().to_string())
                    .unwrap_or_else(|| "A".to_string())
            } else {
                "A".to_string()
            }
        };

        let mut cache_db = Self {
            db: db.clone(),
            active_slot: active_slot.clone(),
            txn: Some(db.begin_write()?),
        };

        // Check legacy JSON and migrate if needed (Best effort)
        let json_path = cache_dir.join(JSON_CACHE_FILE);
        if json_path.exists() {
             cache_db.migrate_legacy_json(&json_path)?;
        }

        cache_db.prepare_next_tables()?;

        Ok(cache_db)
    }
    
    pub fn reader(&self) -> CacheReader {
        CacheReader {
            db: self.db.clone(),
            active_slot: self.active_slot.clone(),
        }
    }

    fn prepare_next_tables(&mut self) -> Result<()> {
        let next_slot = if self.active_slot == "A" { "B" } else { "A" };
        let txn = self.txn.as_mut().unwrap();

        if next_slot == "A" {
            let _ = txn.delete_table(TABLE_DATA_V3_A);
            let _ = txn.delete_table(TABLE_INODES_V3_A);
            txn.open_table(TABLE_DATA_V3_A)?;
            txn.open_table(TABLE_INODES_V3_A)?;
        } else {
            let _ = txn.delete_table(TABLE_DATA_V3_B);
            let _ = txn.delete_table(TABLE_INODES_V3_B);
            txn.open_table(TABLE_DATA_V3_B)?;
            txn.open_table(TABLE_INODES_V3_B)?;
        }
        Ok(())
    }

    fn migrate_legacy_json(&mut self, path: &Path) -> Result<()> {
        println!("{} Migrating JSON cache to Embedded DB...", "📦".cyan());
        if let Ok(file) = File::open(path) {
            if let Ok(cache) = serde_json::from_reader::<_, LegacyVeghCache>(file) {
                 let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                 let txn = self.txn.as_mut().unwrap();
                 
                 // We write to ACTIVE so that the Reader can see it for this run.
                 // But wait, `open` prepares NEXT.
                 // If we migrate, we should probably write to ACTIVE table?
                 // But `CacheDB` logic is: Read Active, Write Next.
                 // If we migrate, we populate the "Active" tables so the current run can use them.
                 
                 let (mut data, mut inodes) = if self.active_slot == "A" {
                     (txn.open_table(TABLE_DATA_V3_A)?, txn.open_table(TABLE_INODES_V3_A)?)
                 } else {
                     (txn.open_table(TABLE_DATA_V3_B)?, txn.open_table(TABLE_INODES_V3_B)?)
                 };
                 
                 for (k, v) in cache.files {
                     let hash_bytes = v.hash.and_then(|h| hex::decode(h).ok()).and_then(|v| v.try_into().ok());
                     let sparse_bytes = v.sparse_hash.and_then(|h| hex::decode(h).ok()).and_then(|v| v.try_into().ok());
                     
                     let mut new_entry = FileCacheEntry {
                         size: v.size,
                         modified: v.modified,
                         inode: v.inode,
                         ctime_sec: 0,
                         ctime_nsec: 0,
                         device_id: 0,
                         last_seen: now,
                         hash: hash_bytes,
                         chunks_compressed: None,
                         sparse_hash: sparse_bytes,
                     };
                     
                     if let Some(chunks) = v.chunks {
                         let bin_chunks: Vec<[u8; 32]> = chunks.into_iter()
                             .filter_map(|h| hex::decode(h).ok().and_then(|v| v.try_into().ok()))
                             .collect();
                         let _ = new_entry.set_chunks(bin_chunks);
                     }
                     
                     let bytes = bincode::serialize(&new_entry)?;
                     data.insert(k.as_str(), bytes.as_slice())?;
                     if new_entry.inode > 0 {
                         inodes.insert(new_entry.inode, k.as_str())?;
                     }
                 }
            }
        }
        let _ = fs::remove_file(path);
        Ok(())
    }

    // Insert into NEXT table
    pub fn insert(&mut self, path: &str, entry: &FileCacheEntry) -> Result<()> {
        let next_slot = if self.active_slot == "A" { "B" } else { "A" };
        let txn = self.txn.as_mut().unwrap();
        
        let bytes = bincode::serialize(entry)?;

        if next_slot == "A" {
            let mut data = txn.open_table(TABLE_DATA_V3_A)?;
            data.insert(path, bytes.as_slice())?;
            if entry.inode > 0 {
                let mut inodes = txn.open_table(TABLE_INODES_V3_A)?;
                inodes.insert(entry.inode, path)?;
            }
        } else {
            let mut data = txn.open_table(TABLE_DATA_V3_B)?;
            data.insert(path, bytes.as_slice())?;
            if entry.inode > 0 {
                let mut inodes = txn.open_table(TABLE_INODES_V3_B)?;
                inodes.insert(entry.inode, path)?;
            }
        }
        Ok(())
    }
    
    // Batch Commit Logic
    pub fn commit_batch(&mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            // We do NOT swap active_slot here. We just commit the data written so far to the NEXT table.
            // The active slot remains the same (Old data), so readers still read old data.
            // The next table gets checkpoints.
            txn.commit()?;
            // Start a new write transaction
            self.txn = Some(self.db.begin_write()?);
        }
        Ok(())
    }

    pub fn garbage_collect_and_merge(&mut self, retention_seconds: u64) -> Result<()> {
        let next_slot = if self.active_slot == "A" { "B" } else { "A" };
        let current_slot = &self.active_slot;
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut entries_to_keep: Vec<(String, Vec<u8>)> = Vec::new();
        
        {
            let txn = self.txn.as_mut().unwrap();
            // Source is ACTIVE
            let source = if current_slot == "A" { 
                match txn.open_table(TABLE_DATA_V3_A) { Ok(t) => Some(t), Err(_) => None }
            } else { 
                match txn.open_table(TABLE_DATA_V3_B) { Ok(t) => Some(t), Err(_) => None }
            };
            
            // Dest is NEXT
            let dest = if next_slot == "A" { txn.open_table(TABLE_DATA_V3_A)? } else { txn.open_table(TABLE_DATA_V3_B)? };

            if let Some(source_table) = source {
                for res in source_table.iter()? {
                    let (k, v) = res?;
                    let key_str = k.value();
                    
                    if dest.get(key_str)?.is_none() {
                        let val_bytes = v.value().to_vec();
                        if let Ok(entry) = bincode::deserialize::<FileCacheEntry>(&val_bytes) {
                            if now.saturating_sub(entry.last_seen) < retention_seconds {
                                entries_to_keep.push((key_str.to_string(), val_bytes));
                            }
                        }
                    }
                }
            }
        } 

        let txn = self.txn.as_mut().unwrap();
        let mut dest_data = if next_slot == "A" { txn.open_table(TABLE_DATA_V3_A)? } else { txn.open_table(TABLE_DATA_V3_B)? };
        let mut dest_inodes = if next_slot == "A" { txn.open_table(TABLE_INODES_V3_A)? } else { txn.open_table(TABLE_INODES_V3_B)? };
        
        for (path, bytes) in entries_to_keep {
            dest_data.insert(path.as_str(), bytes.as_slice())?;
             if let Ok(entry) = bincode::deserialize::<FileCacheEntry>(&bytes) {
                 if entry.inode > 0 {
                     dest_inodes.insert(entry.inode, path.as_str())?;
                 }
             }
        }

        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        let next_slot = if self.active_slot == "A" { "B" } else { "A" };
        
        if let Some(txn) = self.txn.take() {
            {
                let mut meta = txn.open_table(TABLE_META)?;
                meta.insert("active_slot", next_slot)?;
            }
            txn.commit()?;
        }
        Ok(())
    }

    pub fn sync_partial(mut self) -> Result<()> {
        println!("{} Syncing partial cache...", "💾".blue());
        self.garbage_collect_and_merge(u64::MAX)?;
        self.commit()?;
        println!("{} Cache saved (Partial).", "✅".green());
        Ok(())
    }
}
