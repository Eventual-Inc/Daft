use std::{
    fs,
    path::{Path, PathBuf},
};

use common_error::DaftResult;
use uuid::Uuid;

pub struct SpillFile {
    path: PathBuf,
}

impl SpillFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        if self.path.exists()
            && let Err(e) = fs::remove_file(&self.path)
        {
            log::warn!("Failed to remove spill file {}: {}", self.path.display(), e);
        }
    }
}

pub struct SpillManager {
    spill_dir: PathBuf,
}

impl SpillManager {
    pub fn new<P: AsRef<Path>>(spill_dir: P) -> DaftResult<Self> {
        let spill_dir = spill_dir.as_ref().to_path_buf();
        if !spill_dir.exists() {
            fs::create_dir_all(&spill_dir)?;
        }
        Ok(Self { spill_dir })
    }

    pub fn get_temp_spill_file(&self) -> PathBuf {
        let filename = format!("{}.arrow", Uuid::new_v4());
        self.spill_dir.join(filename)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_spill_file_new_and_path() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.arrow");
        fs::write(&file_path, b"data").unwrap();

        let spill_file = SpillFile::new(file_path.clone());
        assert_eq!(spill_file.path(), file_path);
        assert!(file_path.exists());
    }

    #[test]
    fn test_spill_file_drop_removes_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.arrow");
        fs::write(&file_path, b"data").unwrap();
        assert!(file_path.exists());

        {
            let _spill_file = SpillFile::new(file_path.clone());
            // File should still exist while SpillFile is alive
            assert!(file_path.exists());
        }
        // After drop, file should be removed
        assert!(!file_path.exists());
    }

    #[test]
    fn test_spill_file_drop_nonexistent_no_panic() {
        // Dropping a SpillFile for a path that doesn't exist should not panic
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("nonexistent.arrow");
        let _spill_file = SpillFile::new(file_path);
        // Drop happens here — should not panic
    }

    #[test]
    fn test_spill_manager_creates_dir() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path().join("new_spill_dir");
        assert!(!spill_dir.exists());

        let _manager = SpillManager::new(&spill_dir).unwrap();
        assert!(spill_dir.exists());
    }

    #[test]
    fn test_spill_manager_existing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let _manager = SpillManager::new(dir.path()).unwrap();
        // Should not error on existing directory
    }

    #[test]
    fn test_spill_manager_temp_file_unique() {
        let dir = tempfile::tempdir().unwrap();
        let manager = SpillManager::new(dir.path()).unwrap();

        let path1 = manager.get_temp_spill_file();
        let path2 = manager.get_temp_spill_file();

        assert_ne!(path1, path2, "Temp file paths should be unique");
        assert!(path1.to_str().unwrap().ends_with(".arrow"));
        assert!(path2.to_str().unwrap().ends_with(".arrow"));
        assert_eq!(path1.parent().unwrap(), dir.path());
    }
}
