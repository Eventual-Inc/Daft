use crate::{array::DataArray, datatypes::DaftArrowBackedType};
use common_error::DaftResult;

#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftArrowBackedType + 'static,
{
    pub fn size_bytes(&self) -> DaftResult<usize> {
        Ok(arrow2::compute::aggregate::estimated_bytes_size(
            self.data(),
        ))
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn size_bytes(&self) -> DaftResult<usize> {
        use pyo3::prelude::*;
        use pyo3::types::PyList;

        let vector = self.as_arrow().values().to_vec();
        Python::with_gil(|py| {
            let daft_utils = PyModule::import(py, pyo3::intern!(py, "daft.utils"))?;
            let estimate_size_bytes_pylist =
                daft_utils.getattr(pyo3::intern!(py, "estimate_size_bytes_pylist"))?;
            let size_bytes: usize = estimate_size_bytes_pylist
                .call1((PyList::new(py, vector),))?
                .extract()?;
            Ok(size_bytes)
        })
    }
}
