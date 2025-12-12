use std::sync::Arc;

use common_error::DaftResult;
use daft_schema::{dtype::DataType, field::Field};
use pyo3::{Py, PyAny};

use crate::{
    array::growable::{Growable, bitmap_growable::ArrowBitmapGrowable},
    prelude::PythonArray,
    series::{IntoSeries, Series},
};

pub struct PythonGrowable<'a> {
    name: String,
    dtype: DataType,
    arr_refs: Vec<&'a PythonArray>,
    buffer: Vec<Arc<Py<PyAny>>>,
    growable_validity: Option<ArrowBitmapGrowable<'a>>,
}

impl<'a> PythonGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        arr_refs: Vec<&'a PythonArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        let growable_validity = if use_validity || arr_refs.iter().any(|a| a.validity().is_some()) {
            Some(ArrowBitmapGrowable::new(
                arr_refs.iter().map(|a| a.validity()).collect(),
                capacity,
            ))
        } else {
            None
        };

        Self {
            name: name.to_string(),
            dtype: dtype.clone(),
            arr_refs,
            buffer: Vec::with_capacity(capacity),
            growable_validity,
        }
    }
}

impl Growable for PythonGrowable<'_> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let arr = self.arr_refs.get(index).unwrap();
        let arr = arr.slice(start, start + len).unwrap();

        self.buffer.extend_from_slice(arr.values());
        if let Some(growable_validity) = &mut self.growable_validity {
            growable_validity.extend(index, start, len);
        }
    }

    fn add_nulls(&mut self, additional: usize) {
        let pynone = Arc::new(pyo3::Python::attach(|py| py.None()));
        self.buffer.extend(std::iter::repeat_n(pynone, additional));
        if let Some(growable_validity) = &mut self.growable_validity {
            growable_validity.add_nulls(additional);
        }
    }

    fn build(&mut self) -> DaftResult<Series> {
        let field = Arc::new(Field::new(self.name.clone(), self.dtype.clone()));

        let buffer = std::mem::take(&mut self.buffer);

        let grown_validity = std::mem::take(&mut self.growable_validity);
        let built_validity = grown_validity.and_then(|v| v.build());

        Ok(PythonArray::new(field, buffer.into(), built_validity).into_series())
    }
}
