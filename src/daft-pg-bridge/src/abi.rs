//! ABI-owned spill directory lifecycle.

use std::{
    fs::{self, DirBuilder},
    io::ErrorKind,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Mutex,
    },
    thread,
    time::Duration,
};

pub(crate) const SPILL_PENDING: u32 = 1;
pub(crate) const SPILL_CLEANED: u32 = 2;
pub(crate) const SPILL_CLEANUP_FAILED: u32 = 3;

static NEXT_QUERY_DIRECTORY: AtomicU64 = AtomicU64::new(0);

/// Owns the secure per-handle query directory and its idempotent cleanup.
pub(crate) struct SpillDirectory {
    path: PathBuf,
    status: AtomicU32,
    cleanup_lock: Mutex<()>,
}

impl SpillDirectory {
    pub(crate) fn create(base: &str) -> Result<Self, String> {
        let base = Path::new(base);
        if !base.is_absolute() {
            return Err("PGDAFT_SPILL_BASE_NOT_ABSOLUTE".to_string());
        }
        if base
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err("PGDAFT_SPILL_BASE_PATH_INVALID".to_string());
        }

        let metadata =
            fs::symlink_metadata(base).map_err(|_| "PGDAFT_SPILL_BASE_UNAVAILABLE".to_string())?;
        if metadata.file_type().is_symlink() {
            return Err("PGDAFT_SPILL_BASE_SYMLINK".to_string());
        }
        if !metadata.is_dir() {
            return Err("PGDAFT_SPILL_BASE_NOT_DIRECTORY".to_string());
        }
        let canonical =
            fs::canonicalize(base).map_err(|_| "PGDAFT_SPILL_BASE_UNAVAILABLE".to_string())?;
        if canonical != base {
            return Err("PGDAFT_SPILL_BASE_SYMLINK".to_string());
        }
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err("PGDAFT_SPILL_BASE_INSECURE_MODE".to_string());
        }

        for _ in 0..64 {
            let id = NEXT_QUERY_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!("pgdaft-{}-{id:016x}", std::process::id()));
            let mut builder = DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => {
                    let mode = fs::symlink_metadata(&path)
                        .map_err(|_| "PGDAFT_SPILL_QUERY_DIR_VERIFY_FAILED".to_string())?
                        .permissions()
                        .mode()
                        & 0o777;
                    if mode != 0o700 {
                        let _ = fs::remove_dir_all(&path);
                        return Err("PGDAFT_SPILL_QUERY_DIR_MODE_INVALID".to_string());
                    }
                    return Ok(Self {
                        path,
                        status: AtomicU32::new(SPILL_PENDING),
                        cleanup_lock: Mutex::new(()),
                    });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                Err(_) => return Err("PGDAFT_SPILL_QUERY_DIR_CREATE_FAILED".to_string()),
            }
        }
        Err("PGDAFT_SPILL_QUERY_DIR_COLLISION".to_string())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn status(&self) -> u32 {
        self.status.load(Ordering::Acquire)
    }

    pub(crate) fn cleanup(&self) -> u32 {
        let _guard = self.cleanup_lock.lock().unwrap();
        if self.status.load(Ordering::Acquire) != SPILL_PENDING {
            return self.status();
        }
        let mut status = SPILL_CLEANUP_FAILED;
        // Cancellation may catch an IPC writer between create_new() and the
        // worker future observing abort. Retry the exact per-query directory
        // briefly so cleanup wins that bounded race without widening scope.
        for attempt in 0..64 {
            match fs::remove_dir_all(&self.path) {
                Ok(()) => {
                    status = SPILL_CLEANED;
                    break;
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    status = SPILL_CLEANED;
                    break;
                }
                Err(_) if attempt < 63 => thread::sleep(Duration::from_millis(1)),
                Err(_) => {}
            }
        }
        self.status.store(status, Ordering::Release);
        status
    }
}

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        self.cleanup();
    }
}
