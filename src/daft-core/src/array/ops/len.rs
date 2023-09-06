use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::DaftArrowBackedType,
};
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

/// From arrow2 private method (arrow2::compute::aggregate::validity_size)
fn validity_size(validity: Option<&arrow2::bitmap::Bitmap>) -> usize {
    validity.as_ref().map(|b| b.as_slice().0.len()).unwrap_or(0)
}

fn offset_size(offsets: &arrow2::offset::OffsetsBuffer<i64>) -> usize {
    offsets.len_proxy() * std::mem::size_of::<i64>()
}

impl FixedSizeListArray {
    pub fn size_bytes(&self) -> DaftResult<usize> {
        Ok(self.flat_child.size_bytes()? + validity_size(self.validity()))
    }
}

impl ListArray {
    pub fn size_bytes(&self) -> DaftResult<usize> {
        Ok(self.flat_child.size_bytes()?
            + validity_size(self.validity())
            + offset_size(self.offsets()))
    }
}

impl StructArray {
    pub fn size_bytes(&self) -> DaftResult<usize> {
        let children_size_bytes: usize = self
            .children
            .iter()
            .map(|s| s.size_bytes())
            .collect::<DaftResult<Vec<usize>>>()?
            .iter()
            .sum();
        Ok(children_size_bytes + validity_size(self.validity()))
    }
}
