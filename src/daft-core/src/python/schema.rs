use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyTuple;

use super::datatype::PyDataType;
use super::field::PyField;
use crate::datatypes;
use crate::schema;
use crate::schema::Schema;

#[pyclass]
#[derive(Clone)]
pub struct PySchema {
    pub schema: schema::SchemaRef,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (*args))]
    pub fn new(args: &PyTuple) -> PyResult<Self> {
        match args.len() {
            0 => Ok(Self {
                schema: Schema::empty().into(),
            }),
            _ => Err(PyValueError::new_err(format!(
                "expected no arguments to make new PyDataType, got : {}",
                args.len()
            ))),
        }
    }

    pub fn __getitem__(&self, name: &str) -> PyResult<PyField> {
        Ok(self.schema.get_field(name)?.clone().into())
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

    pub fn __setstate__(&mut self, py: Python, state: PyObject) -> PyResult<()> {
        match state.extract::<&PyBytes>(py) {
            Ok(s) => {
                self.schema = bincode::deserialize(s.as_bytes()).unwrap();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn __getstate__(&self, py: Python) -> PyResult<PyObject> {
        Ok(PyBytes::new(py, &bincode::serialize(&self.schema).unwrap()).to_object(py))
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.schema))
    }

    pub fn _repr_html_(&self) -> PyResult<String> {
        Ok(self.schema.repr_html())
    }
}

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
