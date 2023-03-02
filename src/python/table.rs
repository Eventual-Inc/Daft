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

    pub fn filter(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        Ok(self.table.filter(converted_exprs.as_slice())?.into())
    }

    pub fn sort(&self, sort_keys: Vec<PyExpr>, descending: Vec<bool>) -> PyResult<Self> {
        let converted_exprs: Vec<dsl::Expr> = sort_keys.into_iter().map(|e| e.into()).collect();
        Ok(self
            .table
            .sort(converted_exprs.as_slice(), descending.as_slice())?
            .into())
    }

    pub fn argsort(&self, sort_keys: Vec<PyExpr>, descending: Vec<bool>) -> PyResult<PySeries> {
        let converted_exprs: Vec<dsl::Expr> = sort_keys.into_iter().map(|e| e.into()).collect();
        Ok(self
            .table
            .argsort(converted_exprs.as_slice(), descending.as_slice())?
            .into())
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

    pub fn get_column(&self, name: &str) -> PyResult<PySeries> {
        Ok(self.table.get_column(name)?.into())
    }

    pub fn get_column_by_index(&self, idx: i64) -> PyResult<PySeries> {
        if idx < 0 {
            return Err(PyValueError::new_err(format!(
                "Invalid index, negative numbers not supported: {idx}"
            )));
        }
        let idx = idx as usize;
        if idx >= self.table.len() {
            return Err(PyValueError::new_err(format!(
                "Invalid index, out of bounds: {idx} out of {}",
                self.table.len()
            )));
        }

        Ok(self.table.get_column_by_index(idx)?.into())
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

    #[staticmethod]
    pub fn empty() -> PyResult<Self> {
        Ok(table::Table::empty()?.into())
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
