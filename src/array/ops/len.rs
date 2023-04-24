use crate::{
    array::{BaseArray, DataArray},
    datatypes::{DaftArrowBackedType, DaftDataType, PythonArray},
};

use super::downcast::Downcastable;

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn len(&self) -> usize {
        self.data().len()
    }
}

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn size_bytes(&self) -> usize {
        arrow2::compute::aggregate::estimated_bytes_size(self.data())
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn size_bytes(&self) -> usize {
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let vector = self.downcast().values().to_vec();
        Python::with_gil(|py| {
            let daft_utils = PyModule::import(py, pyo3::intern!(py, "daft.utils")).unwrap();
            let estimate_size_bytes_pylist = daft_utils
                .getattr(pyo3::intern!(py, "estimate_size_bytes_pylist"))
                .unwrap();
            let size_bytes: usize = estimate_size_bytes_pylist
                .call1((PyList::new(py, vector),))
                .unwrap()
                .extract()
                .unwrap();
            size_bytes
        })
    }
}
