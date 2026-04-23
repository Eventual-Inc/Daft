//! FFI utilities for converting between PyArrow and Rust Arrow arrays.
//!
//! Portions of this module are adapted from [apache/arrow-rs](https://github.com/apache/arrow-rs),
//! licensed under the Apache License, Version 2.0. We vendor these conversions rather than
//! depending on the `arrow-pyarrow` crate because it pins a different version of `pyo3` than
//! the one used in this workspace, making it incompatible with our codebase.

use std::{
    ffi::CStr,
    ptr::{addr_of, addr_of_mut},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayData, RecordBatch, RecordBatchOptions, RecordBatchReader, StructArray,
        make_array,
    },
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use arrow_schema::{Field, Schema, SchemaRef};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    ffi::Py_uintptr_t,
    import_exception,
    prelude::*,
    pybacked::PyBackedStr,
    types::{PyCapsule, PyList, PyTuple},
};
const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARROW_ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";
const ARROW_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

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

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &CStr) -> PyResult<()> {
    let capsule_name = capsule.name()?;

    if capsule_name.is_none() {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    let capsule_name = unsafe { capsule_name.unwrap().as_cstr() };
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{}' in PyCapsule, instead got '{}'",
            name.to_string_lossy(),
            capsule_name.to_string_lossy(),
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

/// Create a new PyArrow object from a arrow-rs type.
pub trait ToPyArrow {
    /// Convert the implemented type into a Python object without consuming it.
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

/// Convert an arrow-rs type into a PyArrow object.
pub trait IntoPyArrow {
    /// Convert the implemented type into a Python object while consuming it.
    fn into_pyarrow(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>>;
}

impl<T: ToPyArrow> IntoPyArrow for T {
    fn into_pyarrow(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        self.to_pyarrow(py)
    }
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

            validate_pycapsule(schema_capsule, ARROW_SCHEMA_CAPSULE_NAME)?;
            validate_pycapsule(array_capsule, ARROW_ARRAY_CAPSULE_NAME)?;

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
            validate_pycapsule(capsule, ARROW_SCHEMA_CAPSULE_NAME)?;

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

impl ToPyArrow for arrow_schema::DataType {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &raw const c_schema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("DataType")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype)
    }
}

impl ToPyArrow for Field {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &raw const c_schema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("Field")?;
        let dtype = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(dtype)
    }
}

impl ToPyArrow for Schema {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(to_py_err)?;
        let c_schema_ptr = &raw const c_schema;
        let module = py.import("pyarrow")?;
        let class = module.getattr("Schema")?;
        let schema = class.call_method1("_import_from_c", (c_schema_ptr as Py_uintptr_t,))?;
        Ok(schema)
    }
}

impl ToPyArrow for ArrayData {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let array = FFI_ArrowArray::new(self);
        let schema = FFI_ArrowSchema::try_from(self.data_type()).map_err(to_py_err)?;

        let module = py.import("pyarrow")?;
        let class = module.getattr("Array")?;
        let array = class.call_method1(
            "_import_from_c",
            (
                addr_of!(array) as Py_uintptr_t,
                addr_of!(schema) as Py_uintptr_t,
            ),
        )?;
        Ok(array)
    }
}

impl ToPyArrow for RecordBatch {
    fn to_pyarrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Workaround apache/arrow#37669 by returning RecordBatchIterator
        let reader = arrow::array::RecordBatchIterator::new(vec![Ok(self.clone())], self.schema());
        let reader: Box<dyn arrow::array::RecordBatchReader + Send> = Box::new(reader);
        let py_reader = reader.into_pyarrow(py)?;
        py_reader.call_method0("read_next_batch")
    }
}

/// Convert a [`RecordBatchReader`] into a `pyarrow.RecordBatchReader`.
impl IntoPyArrow for Box<dyn arrow::array::RecordBatchReader + Send> {
    // We can't implement `ToPyArrow` for `T: RecordBatchReader + Send` because
    // there is already a blanket implementation for `T: ToPyArrow`.
    fn into_pyarrow(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let mut stream = FFI_ArrowArrayStream::new(self);

        let stream_ptr = &raw mut stream;
        let module = py.import("pyarrow")?;
        let class = module.getattr("RecordBatchReader")?;
        let args = PyTuple::new(py, [stream_ptr as Py_uintptr_t])?;
        let reader = class.call_method1("_import_from_c", args)?;

        Ok(reader)
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

        validate_pycapsule(schema_capsule, ARROW_SCHEMA_CAPSULE_NAME)?;
        validate_pycapsule(array_capsule, ARROW_ARRAY_CAPSULE_NAME)?;

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
        let mut data =
            unsafe { arrow::ffi::from_ffi(array, schema_ptr.as_ref()) }.map_err(to_py_err)?;
        data.align_buffers();

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

    let mut data = unsafe { arrow::ffi::from_ffi(array, &schema) }.map_err(to_py_err)?;
    data.align_buffers();
    let field = arrow_schema::Field::try_from(&schema).map_err(to_py_err)?;

    Ok((data, field))
}

// ---------------------------------------------------------------------------
// PyCapsule export helpers (Arrow PyCapsule Interface)
// ---------------------------------------------------------------------------

/// Extract an Arrow Field from a `requested_schema` PyCapsule argument.
///
/// Per the Arrow PyCapsule spec, `requested_schema` is a PyCapsule wrapping
/// an FFI_ArrowSchema that the consumer wants the producer to cast to.
pub fn field_from_requested_schema(capsule: &Bound<PyCapsule>) -> PyResult<Field> {
    validate_pycapsule(capsule, ARROW_SCHEMA_CAPSULE_NAME)?;
    let schema_ptr = capsule
        .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
        .cast::<FFI_ArrowSchema>();
    unsafe { Field::try_from(schema_ptr.as_ref()).map_err(to_py_err) }
}

/// Extract an Arrow Schema from a `requested_schema` PyCapsule argument.
pub fn schema_from_requested_schema(capsule: &Bound<PyCapsule>) -> PyResult<Schema> {
    validate_pycapsule(capsule, ARROW_SCHEMA_CAPSULE_NAME)?;
    let schema_ptr = capsule
        .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
        .cast::<FFI_ArrowSchema>();
    unsafe { Schema::try_from(schema_ptr.as_ref()).map_err(to_py_err) }
}

/// Export an Arrow Schema as a PyCapsule wrapping FFI_ArrowSchema.
pub fn schema_to_pycapsule(py: Python<'_>, schema: &Schema) -> PyResult<Py<PyAny>> {
    let ffi_schema = FFI_ArrowSchema::try_from(schema).map_err(to_py_err)?;
    Ok(
        PyCapsule::new(py, ffi_schema, Some(ARROW_SCHEMA_CAPSULE_NAME.into()))?
            .into_any()
            .unbind(),
    )
}

/// Export an Arrow Field as a PyCapsule wrapping FFI_ArrowSchema.
pub fn field_to_pycapsule(py: Python<'_>, field: &Field) -> PyResult<Py<PyAny>> {
    let ffi_schema = FFI_ArrowSchema::try_from(field).map_err(to_py_err)?;
    Ok(
        PyCapsule::new(py, ffi_schema, Some(ARROW_SCHEMA_CAPSULE_NAME.into()))?
            .into_any()
            .unbind(),
    )
}

/// Export an Arrow array (ArrayData + Field) as a `(schema_capsule, array_capsule)` tuple.
pub fn array_to_pycapsules(py: Python<'_>, data: &ArrayData, field: &Field) -> PyResult<Py<PyAny>> {
    let ffi_schema = FFI_ArrowSchema::try_from(field).map_err(to_py_err)?;
    let ffi_array = FFI_ArrowArray::new(data);
    let schema_capsule = PyCapsule::new(py, ffi_schema, Some(ARROW_SCHEMA_CAPSULE_NAME.into()))?;
    let array_capsule = PyCapsule::new(py, ffi_array, Some(ARROW_ARRAY_CAPSULE_NAME.into()))?;
    Ok(
        PyTuple::new(py, [schema_capsule.into_any(), array_capsule.into_any()])?
            .into_any()
            .unbind(),
    )
}

/// Export a RecordBatch as a `(schema_capsule, array_capsule)` PyCapsule tuple.
///
/// The RecordBatch is exported as a StructArray at the C level, per the Arrow spec.
pub fn record_batch_to_pycapsules(py: Python<'_>, rb: &RecordBatch) -> PyResult<Py<PyAny>> {
    let struct_array = StructArray::from(rb.clone());
    let data = struct_array.to_data();

    // Build a struct field that mirrors the record batch schema.
    let struct_field = Field::new(
        "",
        arrow_schema::DataType::Struct(rb.schema().fields().clone()),
        false,
    );
    array_to_pycapsules(py, &data, &struct_field)
}

/// Export a RecordBatchReader as a PyCapsule wrapping FFI_ArrowArrayStream.
pub fn reader_to_stream_pycapsule(
    py: Python<'_>,
    reader: Box<dyn RecordBatchReader + Send>,
) -> PyResult<Py<PyAny>> {
    let stream = FFI_ArrowArrayStream::new(reader);
    Ok(
        PyCapsule::new(py, stream, Some(ARROW_STREAM_CAPSULE_NAME.into()))?
            .into_any()
            .unbind(),
    )
}

/// Import Arrow data from a Python object implementing `__arrow_c_stream__`.
///
/// Returns the schema and collected record batches.
pub fn stream_from_python(obj: &Bound<PyAny>) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    if !obj.hasattr("__arrow_c_stream__")? {
        return Err(PyTypeError::new_err(
            "Expected an object implementing __arrow_c_stream__",
        ));
    }

    let capsule = obj.getattr("__arrow_c_stream__")?.call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    validate_pycapsule(capsule, ARROW_STREAM_CAPSULE_NAME)?;

    let stream_ptr = capsule
        .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))?
        .cast::<FFI_ArrowArrayStream>();

    let reader =
        unsafe { ArrowArrayStreamReader::from_raw(stream_ptr.as_ptr()) }.map_err(to_py_err)?;

    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(to_py_err)?;

    Ok((schema, batches))
}
