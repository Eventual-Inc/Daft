use arrow2::array::Array;
use common_error::DaftResult;
use daft_core::{impl_bincode_py_state_serialization, schema::Schema, Series};
use daft_table::Table;
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    daft_table::python::PyTable,
    pyo3::{
        exceptions::PyKeyError, pyclass, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
};

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

    /// Create from a Daft table with "path", "size", and "num_rows" columns.
    #[staticmethod]
    pub fn from_table(table: PyTable) -> PyResult<Self> {
        Ok(Self::from_table_internal(table.table)?)
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

    /// Convert to a Daft table with "path", "size", and "num_rows" columns.
    pub fn to_table(&self) -> PyResult<PyTable> {
        Ok(self.to_table_internal()?.into())
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

    pub fn from_table_internal(table: Table) -> DaftResult<Self> {
        let file_paths = table
            .get_column("path")?
            .utf8()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Utf8Array<i64>>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect::<Vec<_>>();
        let file_sizes = table
            .get_column("size")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.cloned())
            .collect::<Vec<_>>();
        let num_rows = table
            .get_column("num_rows")?
            .i64()?
            .data()
            .as_any()
            .downcast_ref::<arrow2::array::Int64Array>()
            .unwrap()
            .iter()
            .map(|n| n.cloned())
            .collect::<Vec<_>>();
        Ok(Self::new_internal(file_paths, file_sizes, num_rows))
    }

    pub fn len(&self) -> usize {
        self.file_paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file_paths.is_empty()
    }

    pub fn to_table_internal(&self) -> DaftResult<Table> {
        let columns = vec![
            Series::try_from((
                "path",
                arrow2::array::Utf8Array::<i64>::from_iter_values(self.file_paths.iter())
                    .to_boxed(),
            ))?,
            Series::try_from((
                "size",
                arrow2::array::PrimitiveArray::<i64>::from(&self.file_sizes).to_boxed(),
            ))?,
            Series::try_from((
                "num_rows",
                arrow2::array::PrimitiveArray::<i64>::from(&self.num_rows).to_boxed(),
            ))?,
        ];
        Table::new(
            Schema::new(columns.iter().map(|s| s.field().clone()).collect())?,
            columns,
        )
    }
}

impl Default for FileInfos {
    fn default() -> Self {
        Self::new_internal(vec![], vec![], vec![])
    }
}
