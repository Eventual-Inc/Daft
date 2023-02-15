use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::dsl;
use crate::ffi;
use crate::table;

use crate::python::expr::PyExpr;

use super::series::PySeries;

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
            .eval_expression_list(converted_exprs.as_slice())?
            .into())
    }

    pub fn take(&self, idx: &PySeries) -> PyResult<Self> {
        Ok(self.table.take(&idx.series)?.into())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.table))
    }

    pub fn head(&self, num: i64) -> PyResult<Self> {
        if num < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not head table with negative number: {num}"
            )));
        }
        let num = num as usize;
        Ok(self.table.head(num)?.into())
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.table.len())
    }

    pub fn column_names(&self) -> PyResult<Vec<String>> {
        Ok(self.table.column_names()?)
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(record_batches: Vec<&PyAny>) -> PyResult<Self> {
        let table = ffi::record_batches_to_table(record_batches.as_slice())?;
        Ok(PyTable { table })
    }

    pub fn to_arrow_record_batch(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let pyarrow = py.import("pyarrow")?;
            ffi::table_to_record_batch(&self.table, py, pyarrow)
        })
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
