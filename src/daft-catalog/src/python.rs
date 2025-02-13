use std::sync::Arc;

use pyo3::{exceptions::PyIndexError, prelude::*};

use crate::{Catalog, Identifier};

/// PyCatalog implements the Catalog ABC for some Catalog trait impl (rust->py).
#[pyclass]
pub struct PyCatalog(Arc<dyn Catalog>);

impl From<Arc<dyn Catalog>> for PyCatalog {
    fn from(catalog: Arc<dyn Catalog>) -> Self {
        Self(catalog)
    }
}

#[pymethods]
impl PyCatalog {
    fn name(&self) -> String {
        self.0.name()
    }
}

/// PyCatalogImpl implements the Catalog trait for some Catalog ABC impl (py->rust).
#[derive(Debug)]
pub struct PyCatalogImpl(PyObject);

impl From<PyObject> for PyCatalogImpl {
    fn from(obj: PyObject) -> Self {
        Self(obj)
    }
}

impl Catalog for PyCatalogImpl {
    fn name(&self) -> String {
        todo!()
    }

    fn get_table(&self, _name: &Identifier) -> crate::error::Result<Option<Box<dyn crate::Table>>> {
        todo!()
    }
    
    fn to_py(&self, py: Python<'_>) -> PyObject {
        self.0.extract(py).expect("failed to extract PyObject")
    }
}

/// PyIdentifier maps identifier.py to identifier.rs
#[pyclass(sequence)]
#[derive(Debug, Clone)]
pub struct PyIdentifier(Identifier);

#[pymethods]
impl PyIdentifier {
    #[new]
    pub fn new(namespace: Vec<String>, name: String) -> PyIdentifier {
        Identifier::new(namespace, name).into()
    }

    #[staticmethod]
    pub fn from_sql(input: &str, normalize: bool) -> PyResult<PyIdentifier> {
        Ok(Identifier::from_sql(input, normalize)?.into())
    }

    pub fn eq(&self, other: &Self) -> PyResult<bool> {
        Ok(self.0.eq(&other.0))
    }

    pub fn getitem(&self, index: isize) -> PyResult<String> {
        let mut i = index;
        let len = self.__len__()?;
        if i < 0 {
            // negative index
            i = (len as isize) + index;
        }
        if i < 0 || len <= i as usize {
            // out of range
            return Err(PyIndexError::new_err(i));
        }
        if i as usize == len - 1 {
            // last is name
            return Ok(self.0.name.to_string());
        }
        Ok(self.0.namespace[i as usize].to_string())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.0.namespace.len() + 1)
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.0))
    }
}

impl From<Identifier> for PyIdentifier {
    fn from(value: Identifier) -> Self {
        Self(value)
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PyCatalog>()?;
    parent.add_class::<PyIdentifier>()?;
    Ok(())
}
