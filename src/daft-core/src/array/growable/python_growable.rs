use pyo3;

use crate::{DataType, Series};

use super::Growable;

pub struct PythonGrowable<'a> {
    name: String,
    dtype: DataType,
    series_refs: &'a [&'a Series],
    buffer: Vec<pyo3::PyObject>,
}

impl<'a> PythonGrowable<'a> {
    pub fn new(name: String, dtype: &DataType, series_refs: &[&Series], capacity: usize) -> Self {
        Self {
            name,
            dtype: dtype.clone(),
            series_refs,
            buffer: Vec::with_capacity(capacity),
        }
    }
}

impl<'a> Growable for PythonGrowable<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        todo!()
    }

    fn add_nulls(&mut self, additional: usize) {
        todo!()
    }

    fn build(&mut self) -> common_error::DaftResult<Series> {
        todo!()
    }
}
