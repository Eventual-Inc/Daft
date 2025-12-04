use std::collections::HashMap;

use common_py_serde::impl_bincode_py_state_serialization;
#[cfg(feature = "python")]
use pyo3::{PyResult, Python, exceptions::PyKeyError, pyclass, pymethods};
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
    #[pyo3(signature = (file_path, file_size=None, num_rows=None))]
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

    /// Merge two FileInfos together.
    pub fn merge(&mut self, new_infos: Self) {
        let mut index_map = HashMap::new();
        for (i, path) in self.file_paths.iter().enumerate() {
            index_map.insert(path.clone(), i);
        }

        let mut merged_paths = Vec::new();
        let mut merged_sizes = Vec::new();
        let mut merged_rows = Vec::new();
        merged_paths.extend_from_slice(&self.file_paths);
        merged_sizes.extend_from_slice(&self.file_sizes);
        merged_rows.extend_from_slice(&self.num_rows);

        new_infos.into_iter().for_each(|file_info| {
            if let Some(&idx) = index_map.get(&file_info.file_path) {
                merged_sizes[idx] = file_info.file_size;
                merged_rows[idx] = file_info.num_rows;
            } else {
                merged_paths.push(file_info.file_path);
                merged_sizes.push(file_info.file_size);
                merged_rows.push(file_info.num_rows);
            }
        });

        self.file_paths = merged_paths;
        self.file_sizes = merged_sizes;
        self.num_rows = merged_rows;
    }

    pub fn __getitem__(&self, idx: isize) -> PyResult<FileInfo> {
        let idx = idx as usize;
        if idx >= self.len() {
            return Err(PyKeyError::new_err(idx));
        }
        Ok(FileInfo::new_internal(
            self.file_paths[idx].clone(),
            self.file_sizes[idx],
            self.num_rows[idx],
        ))
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.len())
    }
}

impl IntoIterator for FileInfos {
    type Item = FileInfo;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let mut result = Vec::new();
        for i in 0..self.file_paths.len() {
            result.push(FileInfo::new_internal(
                self.file_paths[i].clone(),
                self.file_sizes.get(i).copied().unwrap_or_default(),
                self.num_rows.get(i).copied().unwrap_or_default(),
            ));
        }
        result.into_iter()
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

#[cfg(feature = "python")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extend_no_duplicates() {
        let mut base =
            FileInfos::from_infos(vec!["path1".into()], vec![Some(100)], vec![Some(1000)]);

        let new = FileInfos::from_infos(vec!["path2".into()], vec![Some(200)], vec![Some(2000)]);

        base.merge(new);

        assert_eq!(base.file_paths, vec!["path1", "path2"]);
        assert_eq!(base.file_sizes, vec![Some(100), Some(200)]);
        assert_eq!(base.num_rows, vec![Some(1000), Some(2000)]);
    }

    #[test]
    fn test_extend_multiple_duplicates() {
        let mut base = FileInfos::from_infos(
            vec!["p1".into(), "p2".into(), "p3".into()],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(100), Some(200), Some(300)],
        );

        let new = FileInfos::from_infos(
            vec!["p2".into(), "p1".into(), "p4".into()],
            vec![Some(25), Some(15), Some(40)],
            vec![Some(250), Some(150), Some(400)],
        );

        base.merge(new);

        assert_eq!(base.file_paths, vec!["p1", "p2", "p3", "p4"]);
        assert_eq!(
            base.file_sizes,
            vec![Some(15), Some(25), Some(30), Some(40)]
        );
        assert_eq!(
            base.num_rows,
            vec![Some(150), Some(250), Some(300), Some(400)]
        );
    }

    #[test]
    fn test_extend_with_none_values() {
        let mut base = FileInfos::from_infos(
            vec!["path1".into(), "path2".into()],
            vec![Some(100), Some(200)],
            vec![Some(1000), None],
        );

        let new = FileInfos::from_infos(
            vec!["path1".into(), "path3".into()],
            vec![None, Some(300)],
            vec![None, Some(3000)],
        );

        base.merge(new);

        assert_eq!(base.file_paths, vec!["path1", "path2", "path3"]);
        assert_eq!(base.file_sizes, vec![None, Some(200), Some(300)]);
        assert_eq!(base.num_rows, vec![None, None, Some(3000)]);
    }
}
