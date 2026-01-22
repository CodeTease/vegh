#[cfg(test)]
mod tests {
    use crate::core::{create_snap, restore_snap};
    use anyhow::Result;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_restore_parallel() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_dir = temp_dir.path().join("source");
        let restore_dir = temp_dir.path().join("restore");
        let output_file = temp_dir.path().join("snap.vegh");

        fs::create_dir(&source_dir)?;
        fs::create_dir(&restore_dir)?;

        // Create dummy files
        for i in 0..50 {
            let p = source_dir.join(format!("file_{}.txt", i));
            fs::write(&p, format!("Content of file {}", i))?;
        }

        // Large file for CDC
        let large_path = source_dir.join("large.bin");
        let data = vec![b'x'; 2 * 1024 * 1024]; // 2MB
        fs::write(&large_path, &data)?;

        // Run Create Snap
        let (raw, compressed) = create_snap(
            &source_dir,
            &output_file,
            1,
            Some("Test Snap".to_string()),
            vec![],
            vec![],
            false,
        )?;

        println!("Raw: {}, Compressed: {}", raw, compressed);
        assert!(output_file.exists());

        // Restore
        restore_snap(&output_file, &restore_dir)?;

        // Verify
        for i in 0..50 {
            let p = restore_dir.join(format!("file_{}.txt", i));
            assert!(p.exists());
            let content = fs::read_to_string(p)?;
            assert_eq!(content, format!("Content of file {}", i));
        }

        let restored_large = restore_dir.join("large.bin");
        assert!(restored_large.exists());
        let restored_data = fs::read(restored_large)?;
        assert_eq!(data, restored_data);

        Ok(())
    }

    #[test]
    fn test_incremental_deduplication() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let source_dir = temp_dir.path().join("source");
        let output_file_1 = temp_dir.path().join("snap1.vegh");
        let output_file_2 = temp_dir.path().join("snap2.vegh");

        fs::create_dir(&source_dir)?;

        let p1 = source_dir.join("test.txt");
        fs::write(&p1, "Hello World")?;

        // Run Snap 1
        create_snap(&source_dir, &output_file_1, 1, None, vec![], vec![], false)?;

        // Modify file
        fs::write(&p1, "Hello World Modified")?;

        // Add new file
        let p2 = source_dir.join("test2.txt");
        fs::write(&p2, "New File")?;

        // Run Snap 2
        create_snap(&source_dir, &output_file_2, 1, None, vec![], vec![], false)?;

        // Verify output 2 has correct manifest
        let restore_dir = temp_dir.path().join("restore2");
        restore_snap(&output_file_2, &restore_dir)?;

        assert_eq!(
            fs::read_to_string(restore_dir.join("test.txt"))?,
            "Hello World Modified"
        );
        assert_eq!(
            fs::read_to_string(restore_dir.join("test2.txt"))?,
            "New File"
        );

        Ok(())
    }
}
