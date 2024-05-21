use daft_core::{impl_bincode_py_state_serialization, schema::SchemaRef};
use daft_dsl::ExprRef;
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    daft_core::python::schema::PySchema,
    daft_dsl::python::PyExpr,
    pyo3::{
        pyclass, pyclass::CompareOp, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
};

/// Options for converting JSON data to Daft data.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct JsonConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub schema: Option<SchemaRef>,
    pub predicate: Option<ExprRef>,
}

impl JsonConvertOptions {
    pub fn new_internal(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        schema: Option<SchemaRef>,
        predicate: Option<ExprRef>,
    ) -> Self {
        Self {
            limit,
            include_columns,
            schema,
            predicate,
        }
    }

    pub fn with_limit(self, limit: Option<usize>) -> Self {
        Self { limit, ..self }
    }

    pub fn with_include_columns(self, include_columns: Option<Vec<String>>) -> Self {
        Self {
            include_columns,
            ..self
        }
    }

    pub fn with_schema(self, schema: Option<SchemaRef>) -> Self {
        Self { schema, ..self }
    }

    pub fn with_predicate(self, predicate: Option<ExprRef>) -> Self {
        Self { predicate, ..self }
    }
}

impl Default for JsonConvertOptions {
    fn default() -> Self {
        Self::new_internal(None, None, None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl JsonConvertOptions {
    /// Create conversion options for the JSON reader.
    ///
    /// # Arguments:
    ///
    /// * `limit` - Only read this many rows.
    /// * `include_columns` - The names of the columns that should be kept, e.g. via a projection.
    /// * `schema` - The names and dtypes for the JSON columns.
    /// * `predicate` - Expression to filter rows applied before limit.

    #[new]
    #[pyo3(signature = (limit=None, include_columns=None, schema=None, predicate=None))]
    pub fn new(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        schema: Option<PySchema>,
        predicate: Option<PyExpr>,
    ) -> Self {
        Self::new_internal(
            limit,
            include_columns,
            schema.map(|s| s.into()),
            predicate.map(|p| p.expr),
        )
    }

    #[getter]
    pub fn get_limit(&self) -> PyResult<Option<usize>> {
        Ok(self.limit)
    }

    #[getter]
    pub fn get_include_columns(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.include_columns.clone())
    }

    #[getter]
    pub fn get_schema(&self) -> PyResult<Option<PySchema>> {
        Ok(self.schema.as_ref().map(|s| s.clone().into()))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(JsonConvertOptions);

/// Options for parsing JSON files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonParseOptions {
    pub sample_size: Option<usize>,
}

impl JsonParseOptions {
    pub fn new_internal() -> Self {
        Self { sample_size: None }
    }
}

impl Default for JsonParseOptions {
    fn default() -> Self {
        Self::new_internal()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl JsonParseOptions {
    /// Create parsing options for the JSON reader.
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Self::new_internal())
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(JsonParseOptions);

/// Options for reading JSON files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct JsonReadOptions {
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl JsonReadOptions {
    pub fn new_internal(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size,
        }
    }

    pub fn with_buffer_size(self, buffer_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size: self.chunk_size,
        }
    }

    pub fn with_chunk_size(self, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size: self.buffer_size,
            chunk_size,
        }
    }
}

impl Default for JsonReadOptions {
    fn default() -> Self {
        Self::new_internal(None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl JsonReadOptions {
    /// Create reading options for the JSON reader.
    ///
    /// # Arguments:
    ///
    /// * `buffer_size` - Size of the buffer (in bytes) used by the streaming reader.
    /// * `chunk_size` - Size of the chunks (in bytes) deserialized in parallel by the streaming reader.
    #[new]
    #[pyo3(signature = (buffer_size=None, chunk_size=None))]
    pub fn new(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self::new_internal(buffer_size, chunk_size)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(JsonReadOptions);
