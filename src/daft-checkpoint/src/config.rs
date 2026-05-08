//! Python bindings for live checkpoint stores.
//!
//! The serializable *config* types (`CheckpointStoreConfig`, `CheckpointConfig`)
//! and their `PyCheckpointStoreConfig` wrapper live in `common/checkpoint-config`.
//! This module holds the wrappers for the *live* store objects, plus the free
//! function `build_checkpoint_store()` that bridges config → live store.
//!
//! This module is only compiled under the `python` feature (gated at the
//! `mod config` declaration in `lib.rs`).

use common_checkpoint_config::python::PyCheckpointStoreConfig;
use pyo3::prelude::*;

use crate::CheckpointStoreRef;

/// Construct a live checkpoint store from a Python-side config.
///
/// Replaces the previous `PyCheckpointStoreConfig.build()` method, which would
/// have required `common/checkpoint-config` to depend on `daft-checkpoint` and
/// created a cycle.
#[pyfunction]
pub fn build_checkpoint_store(config: &PyCheckpointStoreConfig) -> PyCheckpointStore {
    PyCheckpointStore {
        store: crate::builder::build_store(&config.config),
    }
}

#[pyclass(module = "daft.daft", name = "PyCheckpointStore", frozen)]
pub struct PyCheckpointStore {
    store: CheckpointStoreRef,
}

#[pymethods]
impl PyCheckpointStore {
    /// List all checkpoints in the store.
    ///
    /// Releases the GIL while the underlying async I/O runs — matches the
    /// `daft-parquet` `py.detach(|| rt.block_on_current_thread(...))` pattern
    /// so we don't stall other Python threads (Ray driver callbacks, etc.)
    /// during S3 list + fan-out GETs.
    pub fn list_checkpoints(&self, py: Python<'_>) -> PyResult<Vec<PyCheckpoint>> {
        use futures::TryStreamExt;
        let store = self.store.clone();
        py.detach(|| {
            let rt = common_runtime::get_io_runtime(true);
            rt.block_on_current_thread(async {
                let stream = store.list_checkpoints().await?;
                let checkpoints: Vec<_> = stream.try_collect().await?;
                Ok(checkpoints.into_iter().map(PyCheckpoint::from).collect())
            })
            .map_err(|e: common_error::DaftError| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
            })
        })
    }

    /// Get file metadata from all checkpointed (but not yet committed) checkpoints.
    pub fn get_checkpointed_files(&self, py: Python<'_>) -> PyResult<Vec<PyFileMetadata>> {
        use futures::TryStreamExt;
        let store = self.store.clone();
        py.detach(|| {
            let rt = common_runtime::get_io_runtime(true);
            rt.block_on_current_thread(async {
                let stream = store.get_checkpointed_files().await?;
                let files: Vec<_> = stream.try_collect().await?;
                Ok(files.into_iter().map(PyFileMetadata::from).collect())
            })
            .map_err(|e: common_error::DaftError| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
            })
        })
    }

    /// Mark checkpoints as committed after a successful catalog commit.
    pub fn mark_committed(&self, py: Python<'_>, ids: Vec<String>) -> PyResult<()> {
        let store = self.store.clone();
        let checkpoint_ids: Vec<crate::CheckpointId> = ids
            .into_iter()
            .map(|s| {
                if !crate::CheckpointId::is_valid(&s) {
                    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "invalid CheckpointId: {s:?}"
                    )))
                } else {
                    Ok(crate::CheckpointId::from_string(s))
                }
            })
            .collect::<PyResult<_>>()?;
        py.detach(|| {
            let rt = common_runtime::get_io_runtime(true);
            rt.block_on_current_thread(async { store.mark_committed(&checkpoint_ids).await })
                .map_err(|e: crate::CheckpointError| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })
        })
    }
}

/// Python-facing enum mirroring [`crate::CheckpointStatus`].
///
/// Exposed as a proper pyclass enum (not a string) so Python callers get
/// autocomplete + typo-safety: `ckpt.status == CheckpointStatus.Checkpointed`
/// rather than `ckpt.status == "checkpointed"`.
#[pyclass(
    module = "daft.daft",
    name = "CheckpointStatus",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyCheckpointStatus {
    Staged,
    Checkpointed,
    Committed,
}

impl From<crate::CheckpointStatus> for PyCheckpointStatus {
    fn from(s: crate::CheckpointStatus) -> Self {
        match s {
            crate::CheckpointStatus::Staged => Self::Staged,
            crate::CheckpointStatus::Checkpointed => Self::Checkpointed,
            crate::CheckpointStatus::Committed => Self::Committed,
        }
    }
}

#[pyclass(module = "daft.daft", name = "PyCheckpoint", frozen)]
pub struct PyCheckpoint {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub status: PyCheckpointStatus,
    #[pyo3(get)]
    pub query_id: String,
}

impl From<crate::Checkpoint> for PyCheckpoint {
    fn from(c: crate::Checkpoint) -> Self {
        Self {
            id: c.id.to_string(),
            status: c.status.into(),
            query_id: c.query_id.to_string(),
        }
    }
}

/// Python-facing enum mirroring [`crate::FileFormat`] (the checkpoint-level
/// tag for opaque file metadata blobs — Iceberg DataFile, Delta AddAction,
/// Parquet path, etc.).
///
/// Named `CheckpointFileFormat` on the Python side to disambiguate from
/// `daft.daft.FileFormat` (the scan/write file format enum — Parquet, CSV,
/// JSON, ...). Different concept, same base name.
#[pyclass(
    module = "daft.daft",
    name = "CheckpointFileFormat",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyCheckpointFileFormat {
    Iceberg,
    Parquet,
    DeltaLake,
}

impl From<crate::FileFormat> for PyCheckpointFileFormat {
    fn from(f: crate::FileFormat) -> Self {
        // Exhaustive match — if a new `FileFormat` variant is added, this
        // will fail to compile, forcing an explicit mapping decision.
        // (The `#[non_exhaustive]` on `FileFormat` is a *cross-crate* guard;
        // in the same crate all variants are visible, so no wildcard needed.)
        match f {
            crate::FileFormat::Iceberg => Self::Iceberg,
            crate::FileFormat::Parquet => Self::Parquet,
            crate::FileFormat::DeltaLake => Self::DeltaLake,
        }
    }
}

#[pyclass(module = "daft.daft", name = "PyFileMetadata", frozen)]
pub struct PyFileMetadata {
    #[pyo3(get)]
    pub format: PyCheckpointFileFormat,
    #[pyo3(get)]
    pub data: Vec<u8>,
}

impl From<crate::FileMetadata> for PyFileMetadata {
    fn from(f: crate::FileMetadata) -> Self {
        Self {
            format: f.format.into(),
            data: f.data,
        }
    }
}
