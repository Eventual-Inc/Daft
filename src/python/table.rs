use pyo3::prelude::*;

use crate::dsl;
use crate::ffi;
use crate::table;

use crate::python::expr::PyExpr;

#[pyclass]
pub struct PyTable {
    pub table: table::Table,
}

#[pymethods]
impl PyTable {
    pub fn eval_expression_list(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        Ok(self
            .table
            .eval_expression_list(converted_exprs.as_slice())
            .unwrap()
            .into())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.table))
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(record_batches: Vec<&PyAny>) -> PyResult<Self> {
        let table = ffi::record_batches_to_table(record_batches.as_slice())?;
        Ok(PyTable { table })
    }
}

impl From<table::Table> for PyTable {
    fn from(value: table::Table) -> Self {
        PyTable { table: value }
    }
}

impl From<PyTable> for table::Table {
    fn from(item: PyTable) -> Self {
        item.table
    }
}
