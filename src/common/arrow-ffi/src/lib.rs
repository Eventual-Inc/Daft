#[cfg(feature = "python")]
use arrow::ffi::FFI_ArrowSchema;
use daft_arrow::array::Array;
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

    let data = unsafe { arrow::ffi::from_ffi(*array, schema.as_ref()) }.map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow array to Rust: {}",
            e
        ))
    })?;

    Ok(daft_arrow::array::from_data(&data))
}

#[cfg(feature = "python")]
pub fn to_py_array<'py>(
    py: Python<'py>,
    array: ArrayRef,
    pyarrow: &Bound<'py, PyModule>,
) -> PyResult<Bound<'py, PyAny>> {
    let field = arrow::ffi::FFI_ArrowSchema::try_from(&arrow_schema::Field::new(
        "",
        array.data_type().clone().into(),
        true,
    ))
    .map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow field to FFI schema: {}",
            e
        ))
    })?;

    let schema = Box::new(field);

    let data = daft_arrow::array::to_data(array.as_ref());
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
    let field = arrow_schema::Field::from(field.clone());

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

    Ok(field.into())
}

#[cfg(feature = "python")]
pub fn dtype_to_py<'py>(
    py: Python<'py>,
    dtype: &daft_arrow::datatypes::DataType,
    pyarrow: Bound<'py, PyModule>,
) -> PyResult<Bound<'py, PyAny>> {
    let field = arrow_schema::Field::new("", dtype.clone().into(), true);

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
