use std::{mem::swap, sync::Arc};

use pyo3;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{Field, PythonArray, PythonType},
    DataType, IntoSeries, Series,
};

use super::Growable;

pub struct PythonGrowable<'a> {
    name: String,
    dtype: DataType,
    arr_refs: Vec<&'a DataArray<PythonType>>,
    buffer: Vec<pyo3::PyObject>,
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

impl<'a> Growable for PythonGrowable<'a> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let arr = self.arr_refs.get(index).unwrap();
        let arr = arr.slice(start, start + len).unwrap();
        let slice_to_copy = arr
            .data()
            .as_any()
            .downcast_ref::<PseudoArrowArray<pyo3::PyObject>>()
            .unwrap();
        let pynone = pyo3::Python::with_gil(|py| py.None());
        for obj in slice_to_copy.iter() {
            match obj {
                None => self.buffer.push(pynone.clone()),
                Some(obj) => self.buffer.push(obj.clone()),
            }
        }
    }
    #[inline]
    fn add_nulls(&mut self, additional: usize) {
        let pynone = pyo3::Python::with_gil(|py| py.None());
        for _ in 0..additional {
            self.buffer.push(pynone.clone());
        }
    }
    #[inline]
    fn build(&mut self) -> common_error::DaftResult<Series> {
        let mut buf: Vec<pyo3::PyObject> = vec![];
        swap(&mut self.buffer, &mut buf);

        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        let arr = PseudoArrowArray::<pyo3::PyObject>::from_pyobj_vec(buf);
        Ok(DataArray::<PythonType>::new(field, Box::new(arr))?.into_series())
    }
}
