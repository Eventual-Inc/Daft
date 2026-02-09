use common_arrow_ffi::ToPyArrow;
use common_error::DaftError;
use pyo3::{PyErr, ffi::Py_uintptr_t, prelude::*};

use crate::series::Series;

impl ToPyArrow for Series {
    fn to_pyarrow<'py>(
        &self,
        py: pyo3::Python<'py>,
    ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
        let array = self.to_arrow()?;
        let target_field = self.field().to_arrow()?;
        let array = if array.data_type() != target_field.data_type() {
            arrow::compute::cast(&array, target_field.data_type()).map_err(DaftError::from)?
        } else {
            array
        };

        let schema = Box::new(arrow::ffi::FFI_ArrowSchema::try_from(target_field).map_err(
            |e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to convert Arrow field to FFI schema: {}",
                    e
                ))
            },
        )?);

        let mut data = array.to_data();
        data.align_buffers();
        let arrow_arr = Box::new(arrow::ffi::FFI_ArrowArray::new(&data));

        let schema_ptr: *const arrow::ffi::FFI_ArrowSchema = &raw const *schema;
        let array_ptr: *const arrow::ffi::FFI_ArrowArray = &raw const *arrow_arr;

        let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
        let array = pyarrow.getattr(pyo3::intern!(py, "Array"))?.call_method1(
            pyo3::intern!(py, "_import_from_c"),
            (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
        )?;

        let array = pyo3::types::PyModule::import(py, pyo3::intern!(py, "daft.arrow_utils"))?
            .getattr(pyo3::intern!(py, "remove_empty_struct_placeholders"))?
            .call1((array,))?;

        Ok(array)
    }
}
