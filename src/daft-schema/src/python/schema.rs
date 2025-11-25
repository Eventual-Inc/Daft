use std::sync::Arc;

use common_py_serde::impl_bincode_py_state_serialization;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use super::{datatype::PyDataType, field::PyField};
use crate::{field::Field, schema};

#[pyclass(module = "daft.daft", eq)]
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pymethods]
impl PySchema {
    pub fn __getitem__(&self, name: &str) -> PyResult<PyField> {
        Ok(self.schema.get_field(name)?.clone().into())
    }

    pub fn to_pyarrow_schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        let field_fn = pyarrow.getattr(pyo3::intern!(py, "field"))?;
        let pyarrow_fields = self
            .schema
            .into_iter()
            .map(|f| {
                // NOTE: Use PyDataType::to_arrow because we need to dip into Python to get
                // the registered Arrow extension types
                let py_dtype: PyDataType = f.dtype.clone().into();
                let py_arrow_dtype = py_dtype.to_arrow(py)?;

                let args = (f.name.as_str(), py_arrow_dtype);
                if !f.metadata.is_empty() {
                    let kwargs = pyo3::types::PyDict::new(py);
                    let metadata_pydict = pyo3::types::PyDict::new(py);
                    for (key, value) in f.metadata.iter() {
                        metadata_pydict.set_item(key, value)?;
                    }
                    kwargs.set_item("metadata", metadata_pydict)?;
                    field_fn.call(args, Some(&kwargs))
                } else {
                    field_fn.call1(args)
                }
            })
            .collect::<PyResult<Vec<_>>>()?;
        pyarrow
            .getattr(pyo3::intern!(py, "schema"))?
            .call1((pyarrow_fields,))
    }

    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn union(&self, other: &Self) -> PyResult<Self> {
        let new_schema = Arc::new(self.schema.union(&other.schema)?);
        Ok(new_schema.into())
    }

    pub fn estimate_row_size_bytes(&self) -> PyResult<f64> {
        Ok(self.schema.estimate_row_size_bytes())
    }

    #[staticmethod]
    pub fn from_field_name_and_types(names_and_types: Vec<(String, PyDataType)>) -> Self {
        let fields = names_and_types
            .iter()
            .map(|(name, pydtype)| Field::new(name, pydtype.clone().into()));
        let schema = schema::Schema::new(fields);
        Self {
            schema: schema.into(),
        }
    }

    #[staticmethod]
    pub fn from_fields(fields: Vec<PyField>) -> Self {
        Self {
            schema: schema::Schema::new(fields.iter().map(|f| f.field.clone())).into(),
        }
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.schema))
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(self.schema.repr_html())
    }

    pub fn _truncated_table_html(&self) -> PyResult<String> {
        Ok(self.schema.truncated_table_html())
    }

    pub fn _truncated_table_string(&self) -> PyResult<String> {
        Ok(self.schema.truncated_table_string())
    }

    pub fn apply_hints(&self, hints: &Self) -> PyResult<Self> {
        let new_schema = Arc::new(self.schema.apply_hints(&hints.schema)?);
        Ok(new_schema.into())
    }

    #[pyo3(name = "display_with_metadata")]
    pub fn display_with_metadata(&self, show_metadata: bool) -> String {
        self.schema.display_with_metadata(show_metadata)
    }

    #[pyo3(name = "min_estimated_size_column")]
    pub fn min_estimated_size_column(&self) -> Option<&str> {
        self.schema.min_estimated_size_column()
    }
}

impl_bincode_py_state_serialization!(PySchema);

impl From<schema::SchemaRef> for PySchema {
    fn from(schema: schema::SchemaRef) -> Self {
        Self { schema }
    }
}

impl From<PySchema> for schema::SchemaRef {
    fn from(item: PySchema) -> Self {
        item.schema
    }
}
