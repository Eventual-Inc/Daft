use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::PyTypeInfo;

use serde::{Deserialize, Serialize};

use super::datatype::PyDataType;
use super::field::PyField;
use crate::datatypes;
use crate::ffi::field_to_py;
use crate::impl_bincode_py_state_serialization;
use crate::schema;

#[pyclass(module = "daft.daft")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pymethods]
impl PySchema {
    pub fn __getitem__(&self, name: &str) -> PyResult<PyField> {
        Ok(self.schema.get_field(name)?.clone().into())
    }

    pub fn to_pyarrow_schema<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        let pyarrow_fields = self
            .schema
            .fields
            .iter()
            .map(|(_, f)| field_to_py(&f.to_arrow()?, py, pyarrow))
            .collect::<PyResult<Vec<_>>>()?;
        pyarrow
            .getattr(pyo3::intern!(py, "schema"))
            .expect("PyArrow module must contain .schema function")
            .call1((pyarrow_fields,))
    }

    pub fn names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn union(&self, other: &PySchema) -> PyResult<PySchema> {
        let new_schema = Arc::new(self.schema.union(&other.schema)?);
        Ok(new_schema.into())
    }

    pub fn eq(&self, other: &PySchema) -> PyResult<bool> {
        Ok(self.schema.fields.eq(&other.schema.fields))
    }

    pub fn estimate_row_size_bytes(&self) -> PyResult<f64> {
        Ok(self.schema.estimate_row_size_bytes())
    }

    #[staticmethod]
    pub fn from_field_name_and_types(
        names_and_types: Vec<(String, PyDataType)>,
    ) -> PyResult<PySchema> {
        let fields = names_and_types
            .iter()
            .map(|(name, pydtype)| datatypes::Field::new(name, pydtype.clone().into()))
            .collect();
        let schema = schema::Schema::new(fields)?;
        Ok(PySchema {
            schema: schema.into(),
        })
    }

    #[staticmethod]
    pub fn from_fields(fields: Vec<PyField>) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: schema::Schema::new(fields.iter().map(|f| f.field.clone()).collect())?.into(),
        })
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
}

impl_bincode_py_state_serialization!(PySchema);

impl From<schema::SchemaRef> for PySchema {
    fn from(schema: schema::SchemaRef) -> Self {
        PySchema { schema }
    }
}

impl From<PySchema> for schema::SchemaRef {
    fn from(item: PySchema) -> Self {
        item.schema
    }
}
