#![allow(deprecated, reason = "arrow2->arrow migration")]

use std::{ffi::CStr, ptr::addr_of_mut, sync::Arc};

use arrow::{
    array::{ArrayData, RecordBatch, RecordBatchOptions, StructArray, make_array},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};
use arrow_schema::{Field, Schema};
use daft_arrow::{array::Array, datatypes::arrow2_field_to_arrow};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    ffi::Py_uintptr_t,
    import_exception,
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyCapsule, PyList, PyTuple},
};
pub type ArrayRef = Box<dyn Array>;
const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARROW_ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";

import_exception!(pyarrow, ArrowException);
/// Represents an exception raised by PyArrow.
pub type PyArrowException = ArrowException;

fn to_py_err(err: arrow::error::ArrowError) -> PyErr {
    PyArrowException::new_err(err.to_string())
}

fn validate_class(expected: &str, value: &Bound<PyAny>) -> PyResult<()> {
    let pyarrow = PyModule::import(value.py(), "pyarrow")?;
    let class = pyarrow.getattr(expected)?;
    if !value.is_instance(&class)? {
        let expected_module = class.getattr("__module__")?.extract::<PyBackedStr>()?;
        let expected_name = class.getattr("__name__")?.extract::<PyBackedStr>()?;
        let found_class = value.get_type();
        let found_module = found_class
            .getattr("__module__")?
            .extract::<PyBackedStr>()?;
        let found_name = found_class.getattr("__name__")?.extract::<PyBackedStr>()?;
        return Err(PyTypeError::new_err(format!(
            "Expected instance of {expected_module}.{expected_name}, got {found_module}.{found_name}",
        )));
    }
    Ok(())
}

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;

    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = unsafe { capsule_name.unwrap().as_cstr().to_str()? };
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'",
        )));
    }

    Ok(())
}

/// Trait for converting Python objects to arrow-rs types.
pub trait FromPyArrow: Sized {
    /// Convert a Python object to an arrow-rs type.
    ///
    /// Takes a GIL-bound value from Python and returns a result with the arrow-rs type.
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self>;
}

impl FromPyArrow for ArrayData {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        let (data, _) = array_to_rust(value)?;
        Ok(data)
    }
}

impl FromPyArrow for RecordBatch {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

        if value.hasattr("__arrow_c_array__")? {
            let tuple = value.getattr("__arrow_c_array__")?.call0()?;

            if !tuple.is_instance_of::<PyTuple>() {
                return Err(PyTypeError::new_err(
                    "Expected __arrow_c_array__ to return a tuple.",
                ));
            }

            let schema_capsule = tuple.get_item(0)?;
            let schema_capsule = schema_capsule.cast::<PyCapsule>()?;
            let array_capsule = tuple.get_item(1)?;
            let array_capsule = array_capsule.cast::<PyCapsule>()?;

            validate_pycapsule(schema_capsule, "arrow_schema")?;
            validate_pycapsule(array_capsule, "arrow_array")?;

            let schema_ptr = schema_capsule
                .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
                .cast::<FFI_ArrowSchema>();
            let array_ptr = array_capsule
                .pointer_checked(Some(ARROW_ARRAY_CAPSULE_NAME))?
                .cast::<FFI_ArrowArray>();
            let ffi_array = unsafe { FFI_ArrowArray::from_raw(array_ptr.as_ptr()) };
            let mut array_data = unsafe { arrow::ffi::from_ffi(ffi_array, schema_ptr.as_ref()) }
                .map_err(to_py_err)?;
            if !matches!(
                array_data.data_type(),
                arrow::datatypes::DataType::Struct(_)
            ) {
                return Err(PyTypeError::new_err(format!(
                    "Expected Struct type from __arrow_c_array., got {:?}",
                    array_data.data_type()
                )));
            }
            let options = RecordBatchOptions::default().with_row_count(Some(array_data.len()));
            // Ensure data is aligned (by potentially copying the buffers).
            // This is needed because some python code (for example the
            // python flight client) produces unaligned buffers
            // See https://github.com/apache/arrow/issues/43552 for details
            array_data.align_buffers();
            let array = StructArray::from(array_data);
            // StructArray does not embed metadata from schema. We need to override
            // the output schema with the schema from the capsule.
            let schema =
                unsafe { Arc::new(Schema::try_from(schema_ptr.as_ref()).map_err(to_py_err)?) };
            let (_fields, columns, nulls) = array.into_parts();
            assert_eq!(
                nulls.map(|n| n.null_count()).unwrap_or_default(),
                0,
                "Cannot convert nullable StructArray to RecordBatch, see StructArray documentation"
            );
            return Self::try_new_with_options(schema, columns, &options).map_err(to_py_err);
        }

        validate_class("RecordBatch", value)?;
        // TODO(kszucs): implement the FFI conversions in arrow-rs for RecordBatches
        let schema = value.getattr("schema")?;
        let schema = Arc::new(Schema::from_pyarrow_bound(&schema)?);

        let arrays = value.getattr("columns")?;
        let arrays = arrays
            .cast::<PyList>()?
            .iter()
            .map(|a| Ok(make_array(ArrayData::from_pyarrow_bound(&a)?)))
            .collect::<PyResult<_>>()?;

        let row_count = value
            .getattr("num_rows")
            .ok()
            .and_then(|x| x.extract().ok());
        let options = RecordBatchOptions::default().with_row_count(row_count);

        let batch = Self::try_new_with_options(schema, arrays, &options).map_err(to_py_err)?;
        Ok(batch)
    }
}

impl FromPyArrow for Schema {
    fn from_pyarrow_bound(value: &Bound<PyAny>) -> PyResult<Self> {
        // Newer versions of PyArrow as well as other libraries with Arrow data implement this
        // method, so prefer it over _export_to_c.
        // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        if value.hasattr("__arrow_c_schema__")? {
            let capsule = value.getattr("__arrow_c_schema__")?.call0()?;
            let capsule = capsule.cast::<PyCapsule>()?;
            validate_pycapsule(capsule, "arrow_schema")?;

            let schema_ptr = capsule
                .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
                .cast::<FFI_ArrowSchema>();
            unsafe {
                let schema = Self::try_from(schema_ptr.as_ref()).map_err(to_py_err)?;
                return Ok(schema);
            }
        }

        validate_class("Schema", value)?;

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &raw const c_schema;
        value.call_method1("_export_to_c", (c_schema_ptr as Py_uintptr_t,))?;
        let schema = Self::try_from(&c_schema).map_err(to_py_err)?;
        Ok(schema)
    }
}

pub fn array_to_rust(value: &Bound<PyAny>) -> PyResult<(ArrayData, Field)> {
    // Newer versions of PyArrow as well as other libraries with Arrow data implement this
    // method, so prefer it over _export_to_c.
    // See https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
    if value.hasattr("__arrow_c_array__")? {
        let tuple = value.getattr("__arrow_c_array__")?.call0()?;

        if !tuple.is_instance_of::<PyTuple>() {
            return Err(PyTypeError::new_err(
                "Expected __arrow_c_array__ to return a tuple.",
            ));
        }

        let schema_capsule = tuple.get_item(0)?;
        let schema_capsule = schema_capsule.cast::<PyCapsule>()?;
        let array_capsule = tuple.get_item(1)?;
        let array_capsule = array_capsule.cast::<PyCapsule>()?;

        validate_pycapsule(schema_capsule, "arrow_schema")?;
        validate_pycapsule(array_capsule, "arrow_array")?;

        let schema_ptr = schema_capsule
            .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
            .cast::<FFI_ArrowSchema>();
        let array = unsafe {
            FFI_ArrowArray::from_raw(
                array_capsule
                    .pointer_checked(Some(ARROW_ARRAY_CAPSULE_NAME))?
                    .cast::<FFI_ArrowArray>()
                    .as_ptr(),
            )
        };
        let data =
            unsafe { arrow::ffi::from_ffi(array, schema_ptr.as_ref()) }.map_err(to_py_err)?;

        let field =
            unsafe { arrow_schema::Field::try_from(schema_ptr.as_ref()).map_err(to_py_err)? };

        return Ok((data, field));
    }

    validate_class("Array", value)?;

    // prepare a pointer to receive the Array struct
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe.
    // In particular, `_export_to_c` can go out of bounds
    value.call_method1(
        "_export_to_c",
        (
            addr_of_mut!(array) as Py_uintptr_t,
            addr_of_mut!(schema) as Py_uintptr_t,
        ),
    )?;

    let data = unsafe { arrow::ffi::from_ffi(array, &schema) }.map_err(to_py_err)?;
    let field = arrow_schema::Field::try_from(&schema).map_err(to_py_err)?;

    Ok((data, field))
}

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

/// Export an arrow-rs array to PyArrow
///
/// Uses a caller-provided Field for the schema.
/// This preserves extension type metadata that would otherwise be lost when
/// deriving the schema from the array's DataType alone.
pub fn to_py_array_v2<'py>(
    py: Python<'py>,
    array: arrow::array::ArrayRef,
    field: &arrow_schema::Field,
) -> PyResult<Bound<'py, PyAny>> {
    let schema = Box::new(arrow::ffi::FFI_ArrowSchema::try_from(field).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to convert Arrow field to FFI schema: {}",
            e
        ))
    })?);

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

    let array = PyModule::import(py, pyo3::intern!(py, "daft.arrow_utils"))?
        .getattr(pyo3::intern!(py, "remove_empty_struct_placeholders"))?
        .call1((array,))?;

    Ok(array)
}

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
