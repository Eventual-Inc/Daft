use common_py_serde::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyKeyError, pyclass, pymethods, PyObject, PyResult, Python};
use serde::{Deserialize, Serialize};

/// Metadata for a single file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct FileInfo {
    pub file_path: String,
    pub file_size: Option<i64>,
    pub num_rows: Option<i64>,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileInfo {
    #[new]
    pub fn new(file_path: String, file_size: Option<i64>, num_rows: Option<i64>) -> Self {
        Self::new_internal(file_path, file_size, num_rows)
    }
}

impl FileInfo {
    pub fn new_internal(file_path: String, file_size: Option<i64>, num_rows: Option<i64>) -> Self {
        Self {
            file_path,
            file_size,
            num_rows,
        }
    }
}

/// Metadata for a collection of files.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct FileInfos {
    pub file_paths: Vec<String>,
    pub file_sizes: Vec<Option<i64>>,
    pub num_rows: Vec<Option<i64>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl FileInfos {
    #[new]
    pub fn new() -> Self {
        Default::default()
    }

    #[staticmethod]
    pub fn from_infos(
        file_paths: Vec<String>,
        file_sizes: Vec<Option<i64>>,
        num_rows: Vec<Option<i64>>,
    ) -> Self {
        Self::new_internal(file_paths, file_sizes, num_rows)
    }

    /// Concatenate two FileInfos together.
    pub fn extend(&mut self, new_infos: Self) {
        self.file_paths.extend(new_infos.file_paths);
        self.file_sizes.extend(new_infos.file_sizes);
        self.num_rows.extend(new_infos.num_rows);
    }

    pub fn __getitem__(&self, idx: isize) -> PyResult<FileInfo> {
        if idx as usize >= self.len() {
            return Err(PyKeyError::new_err(idx));
        }
        Ok(FileInfo::new_internal(
            self.file_paths[0].clone(),
            self.file_sizes[0],
            self.num_rows[0],
        ))
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.len())
    }
}

impl_bincode_py_state_serialization!(FileInfos);

impl FileInfos {
    pub fn new_internal(
        file_paths: Vec<String>,
        file_sizes: Vec<Option<i64>>,
        num_rows: Vec<Option<i64>>,
    ) -> Self {
        Self {
            file_paths,
            file_sizes,
            num_rows,
        }
    }

    pub fn len(&self) -> usize {
        self.file_paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file_paths.is_empty()
    }
}

impl Default for FileInfos {
    fn default() -> Self {
        Self::new_internal(vec![], vec![], vec![])
    }
}
