#![allow(deprecated, reason = "arrow2->arrow migration")]

#[cfg(feature = "python")]
use arrow::ffi::FFI_ArrowSchema;
#[cfg(feature = "python")]
use daft_arrow::datatypes::arrow2_field_to_arrow;
use daft_arrow::{array::Array, datatypes::DataType as Arrow2DataType};
#[cfg(feature = "python")]
use pyo3::ffi::Py_uintptr_t;
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub type ArrayRef = Box<dyn Array>;

#[cfg(feature = "python")]
pub fn array_to_rust(py: Python, arrow_array: Bound<PyAny>) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(arrow::ffi::FFI_ArrowArray::empty());
    let schema = Box::new(arrow::ffi::FFI_ArrowSchema::empty());

    let array_ptr = &raw const *array;
    let schema_ptr = &raw const *schema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    arrow_array.call_method1(
        pyo3::intern!(py, "_export_to_c"),
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    let mut data = unsafe { arrow::ffi::from_ffi(*array, schema.as_ref()) }.map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow array to Rust: {}",
            e
        ))
    })?;
    data.align_buffers();
    // this DOES not properly preserve metadata, so we need to update the fields with any extension types.
    let mut arr = daft_arrow::array::from_data(&data);

    let field = arrow_schema::Field::try_from(schema.as_ref()).unwrap();
    if let Some(ext_name) = field.metadata().get("ARROW:extension:name") {
        let metadata = field.metadata().get("ARROW:extension:metadata");
        // I (cory) believe this would fail if you had nested extension types.
        // I'm not sure we (or others) use nested extension types, or if this is even supported by arrow
        let dtype = Arrow2DataType::Extension(
            ext_name.clone(),
            Box::new(field.data_type().clone().into()),
            metadata.cloned(),
        );
        arr.change_type(dtype);
    }

    Ok(arr)
}

#[cfg(feature = "python")]
pub fn to_py_array<'py>(
    py: Python<'py>,
    array: ArrayRef,
    pyarrow: &Bound<'py, PyModule>,
) -> PyResult<Bound<'py, PyAny>> {
    let field = daft_arrow::datatypes::Field::new("", array.data_type().clone(), true);
    let field = arrow2_field_to_arrow(field);
    let field = arrow::ffi::FFI_ArrowSchema::try_from(&field).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow field to FFI schema: {}",
            e
        ))
    })?;

    let schema = Box::new(field);

    let mut data = daft_arrow::array::to_data(array.as_ref());
    data.align_buffers();
    let arrow_arr = Box::new(arrow::ffi::FFI_ArrowArray::new(&data));

    let schema_ptr: *const arrow::ffi::FFI_ArrowSchema = &raw const *schema;
    let array_ptr: *const arrow::ffi::FFI_ArrowArray = &raw const *arrow_arr;

    let array = pyarrow.getattr(pyo3::intern!(py, "Array"))?.call_method1(
        pyo3::intern!(py, "_import_from_c"),
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    let array = PyModule::import(py, pyo3::intern!(py, "daft.arrow_utils"))?
        .getattr(pyo3::intern!(py, "remove_empty_struct_placeholders"))?
        .call1((array,))?;

    Ok(array)
}

#[cfg(feature = "python")]
pub fn field_to_py(
    py: Python,
    field: &daft_arrow::datatypes::Field,
    pyarrow: &Bound<PyModule>,
) -> PyResult<Py<PyAny>> {
    let schema = Box::new(
        FFI_ArrowSchema::try_from(arrow2_field_to_arrow(field.clone())).map_err(|e| {
            use pyo3::exceptions::PyRuntimeError;

            PyErr::new::<PyRuntimeError, _>(format!(
                "Failed to convert field to FFI_ArrowSchema: {}",
                e
            ))
        })?,
    );

    let schema_ptr: *const FFI_ArrowSchema = &raw const *schema;

    let field = pyarrow.getattr(pyo3::intern!(py, "Field"))?.call_method1(
        pyo3::intern!(py, "_import_from_c"),
        (schema_ptr as Py_uintptr_t,),
    )?;

    Ok(field.into())
}

#[cfg(feature = "python")]
pub fn dtype_to_py<'py>(
    py: Python<'py>,
    dtype: &daft_arrow::datatypes::DataType,
    pyarrow: Bound<'py, PyModule>,
) -> PyResult<Bound<'py, PyAny>> {
    let field = daft_arrow::datatypes::Field::new("", dtype.clone(), true);
    let field = arrow2_field_to_arrow(field);

    let schema = Box::new(FFI_ArrowSchema::try_from(field).map_err(|e| {
        use pyo3::exceptions::PyRuntimeError;

        PyErr::new::<PyRuntimeError, _>(format!(
            "Failed to convert field to FFI_ArrowSchema: {}",
            e
        ))
    })?);
    let schema_ptr: *const FFI_ArrowSchema = &raw const *schema;

    let field = pyarrow.getattr(pyo3::intern!(py, "Field"))?.call_method1(
        pyo3::intern!(py, "_import_from_c"),
        (schema_ptr as Py_uintptr_t,),
    )?;

    field.getattr(pyo3::intern!(py, "type"))
}
