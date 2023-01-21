use pyo3::prelude::*;

use crate::table;

#[pyclass]
pub struct PyTable {
    pub table: table::Table,
}
