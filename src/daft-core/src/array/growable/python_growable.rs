use std::{mem::swap, sync::Arc};

use pyo3;

use crate::{
    array::{pseudo_arrow::PseudoArrowArray, DataArray},
    datatypes::{Field, PythonType, PythonArray},
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
        name: String,
        dtype: &DataType,
        series_refs: &'a [&'a Series],
        capacity: usize,
    ) -> Self {
        for s in series_refs.iter() {
            if s.data_type() != &DataType::Python {
                panic!("PythonGrowable expected all Series to be of DataType::Python, but received: {}", s.data_type());
            }
        }
        let arr_refs = series_refs
            .iter()
            .map(|s| s.downcast::<PythonArray>().unwrap())
            .collect::<Vec<_>>();
        Self {
            name,
            dtype: dtype.clone(),
            arr_refs,
            buffer: Vec::with_capacity(capacity),
        }
    }
}

impl<'a> Growable for PythonGrowable<'a> {
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

    fn add_nulls(&mut self, additional: usize) {
        let pynone = pyo3::Python::with_gil(|py| py.None());
        for _ in 0..additional {
            self.buffer.push(pynone.clone());
        }
    }

    fn build(&mut self) -> common_error::DaftResult<Series> {
        let mut buf: Vec<pyo3::PyObject> = vec![];
        swap(&mut self.buffer, &mut buf);

        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));
        let arr = PseudoArrowArray::<pyo3::PyObject>::from_pyobj_vec(buf);
        let arr = DataArray::<PythonType>::new(field, Box::new(arr))?;
        Ok(arr.into_series())
    }
}
