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

/// Options for converting CSV data to Daft data.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct CsvConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub column_names: Option<Vec<String>>,
    pub schema: Option<SchemaRef>,
    pub predicate: Option<ExprRef>,
}

impl CsvConvertOptions {
    pub fn new_internal(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        column_names: Option<Vec<String>>,
        schema: Option<SchemaRef>,
        predicate: Option<ExprRef>,
    ) -> Self {
        Self {
            limit,
            include_columns,
            column_names,
            schema,
            predicate,
        }
    }

    pub fn with_limit(self, limit: Option<usize>) -> Self {
        Self {
            limit,
            include_columns: self.include_columns,
            column_names: self.column_names,
            schema: self.schema,
            predicate: self.predicate,
        }
    }

    pub fn with_include_columns(self, include_columns: Option<Vec<String>>) -> Self {
        Self {
            limit: self.limit,
            include_columns,
            column_names: self.column_names,
            schema: self.schema,
            predicate: self.predicate,
        }
    }

    pub fn with_column_names(self, column_names: Option<Vec<String>>) -> Self {
        Self {
            limit: self.limit,
            include_columns: self.include_columns,
            column_names,
            schema: self.schema,
            predicate: self.predicate,
        }
    }

    pub fn with_schema(self, schema: Option<SchemaRef>) -> Self {
        Self {
            limit: self.limit,
            include_columns: self.include_columns,
            column_names: self.column_names,
            schema,
            predicate: self.predicate,
        }
    }
}

impl Default for CsvConvertOptions {
    fn default() -> Self {
        Self::new_internal(None, None, None, None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvConvertOptions {
    /// Create conversion options for the CSV reader.
    ///
    /// # Arguments:
    ///
    /// * `limit` - Only read this many rows.
    /// * `include_columns` - The names of the columns that should be kept, e.g. via a projection.
    /// * `column_names` - The names for the CSV columns.
    /// * `schema` - The names and dtypes for the CSV columns.
    /// * `predicate` - Expression to filter rows applied before the limit
    #[new]
    #[pyo3(signature = (limit=None, include_columns=None, column_names=None, schema=None, predicate=None))]
    pub fn new(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        column_names: Option<Vec<String>>,
        schema: Option<PySchema>,
        predicate: Option<PyExpr>,
    ) -> Self {
        Self::new_internal(
            limit,
            include_columns,
            column_names,
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
    pub fn get_column_names(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.column_names.clone())
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

impl_bincode_py_state_serialization!(CsvConvertOptions);

/// Options for parsing CSV files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvParseOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub double_quote: bool,
    pub quote: u8,
    pub escape_char: Option<u8>,
    pub comment: Option<u8>,
    pub allow_variable_columns: bool,
}

impl CsvParseOptions {
    pub fn new_internal(
        has_header: bool,
        delimiter: u8,
        double_quote: bool,
        quote: u8,
        allow_variable_columns: bool,
        escape_char: Option<u8>,
        comment: Option<u8>,
    ) -> Self {
        Self {
            has_header,
            delimiter,
            double_quote,
            quote,
            allow_variable_columns,
            escape_char,
            comment,
        }
    }

    pub fn new_with_defaults(
        has_header: bool,
        delimiter: Option<char>,
        double_quote: bool,
        quote: Option<char>,
        allow_variable_columns: bool,
        escape_char: Option<char>,
        comment: Option<char>,
    ) -> super::Result<Self> {
        Ok(Self::new_internal(
            has_header,
            char_to_byte(delimiter)?.unwrap_or(b','),
            double_quote,
            char_to_byte(quote)?.unwrap_or(b'"'),
            allow_variable_columns,
            char_to_byte(escape_char)?,
            char_to_byte(comment)?,
        ))
    }

    pub fn with_has_header(self, has_header: bool) -> Self {
        Self { has_header, ..self }
    }

    pub fn with_delimiter(self, delimiter: u8) -> Self {
        Self { delimiter, ..self }
    }

    pub fn with_double_quote(self, double_quote: bool) -> Self {
        Self {
            double_quote,
            ..self
        }
    }

    pub fn with_quote(self, quote: u8) -> Self {
        Self { quote, ..self }
    }

    pub fn with_escape_char(self, escape_char: Option<u8>) -> Self {
        Self {
            escape_char,
            ..self
        }
    }

    pub fn with_comment(self, comment: Option<u8>) -> Self {
        Self { comment, ..self }
    }

    pub fn with_variable_columns(self, allow_variable_columns: bool) -> Self {
        Self {
            allow_variable_columns,
            ..self
        }
    }
}

impl Default for CsvParseOptions {
    fn default() -> Self {
        Self::new_with_defaults(true, None, true, None, false, None, None).unwrap()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvParseOptions {
    /// Create parsing options for the CSV reader.
    ///
    /// # Arguments:
    ///
    /// * `has_headers` - Whether the CSV has a header row; if so, it will be skipped during data parsing.
    /// * `delimiter` - The character delmiting individual cells in the CSV data.
    /// * `double_quote` - Whether double-quote escapes are enabled.
    /// * `quote` - The character to use for quoting strings.
    /// * `escape_char` - The character to use as an escape character.
    /// * `comment` - The character at the start of a line that indicates that the rest of the line is a comment,
    ///   which should be ignored while parsing.
    #[new]
    #[pyo3(signature = (has_header=true, delimiter=None, double_quote=false, quote=None, allow_variable_columns=false, escape_char=None, comment=None))]
    pub fn new(
        has_header: bool,
        delimiter: Option<char>,
        double_quote: bool,
        quote: Option<char>,
        allow_variable_columns: bool,
        escape_char: Option<char>,
        comment: Option<char>,
    ) -> PyResult<Self> {
        Ok(Self::new_with_defaults(
            has_header,
            delimiter,
            double_quote,
            quote,
            allow_variable_columns,
            escape_char,
            comment,
        )?)
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

pub fn char_to_byte(c: Option<char>) -> Result<Option<u8>, super::Error> {
    match c.map(u8::try_from).transpose() {
        Ok(b) => Ok(b),
        Err(e) => Err(super::Error::WrongChar {
            source: e,
            val: c.unwrap_or(' '),
        }),
    }
}

impl_bincode_py_state_serialization!(CsvParseOptions);

/// Options for reading CSV files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvReadOptions {
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl CsvReadOptions {
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

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self::new_internal(None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvReadOptions {
    /// Create reading options for the CSV reader.
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

impl_bincode_py_state_serialization!(CsvReadOptions);
