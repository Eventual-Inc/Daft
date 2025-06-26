use std::{mem::swap, sync::Arc};

use super::Growable;
use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{DataType, Field, PythonArray, PythonType},
    series::{IntoSeries, Series},
};

pub struct PythonGrowable<'a> {
    name: String,
    dtype: DataType,
    arr_refs: Vec<&'a DataArray<PythonType>>,
    buffer: Vec<Arc<pyo3::PyObject>>,
}

impl<'a> PythonGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arr_refs: Vec<&'a PythonArray>,
        capacity: usize,
    ) -> Self {
        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            arr_refs,
            buffer: Vec::with_capacity(capacity),
        }
    }
}

impl Growable for PythonGrowable<'_> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let arr = self.arr_refs.get(index).unwrap();
        let arr = arr.slice(start, start + len).unwrap();
        let slice_to_copy = arr
            .data()
            .as_any()
            .downcast_ref::<PseudoArrowArray<Arc<pyo3::PyObject>>>()
            .unwrap();
        let pynone = Arc::new(pyo3::Python::with_gil(|py| py.None()));
        for obj in slice_to_copy.iter() {
            match obj {
                None => self.buffer.push(pynone.clone()),
                Some(obj) => self.buffer.push(obj.clone()),
            }
        }
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        let pynone = Arc::new(pyo3::Python::with_gil(|py| py.None()));
        for _ in 0..additional {
            self.buffer.push(pynone.clone());
        }
    }
    #[inline]
    fn build(&mut self) -> common_error::DaftResult<Series> {
        let mut buf: Vec<Arc<pyo3::PyObject>> = vec![];
        swap(&mut self.buffer, &mut buf);

        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        let arr = PseudoArrowArray::from_pyobj_vec(buf);
        Ok(DataArray::<PythonType>::new(field, Box::new(arr))?.into_series())
    }
}
