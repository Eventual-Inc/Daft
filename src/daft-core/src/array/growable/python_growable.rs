use pyo3;

use crate::{DataType, Series};

use super::Growable;

pub struct PythonGrowable<'a> {
    _name: String,
    _dtype: DataType,
    _series_refs: &'a [&'a Series],
    _buffer: Vec<pyo3::PyObject>,
}

impl<'a> PythonGrowable<'a> {
    pub fn new(
        name: String,
        dtype: &DataType,
        series_refs: &'a [&'a Series],
        capacity: usize,
    ) -> Self {
        Self {
            _name: name,
            _dtype: dtype.clone(),
            _series_refs: series_refs,
            _buffer: Vec::with_capacity(capacity),
        }
    }
}

impl<'a> Growable for PythonGrowable<'a> {
    fn extend(&mut self, _index: usize, _start: usize, _len: usize) {
        todo!()
    }

    fn add_nulls(&mut self, _additional: usize) {
        todo!()
    }

    fn build(&mut self) -> common_error::DaftResult<Series> {
        todo!()
    }
}
