use pyo3::prelude::*;

use crate::table;

#[pyclass]
pub struct PyTable {
    pub table: table::Table,
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
