use std::io::Cursor;

use arrow2::{array::Array, datatypes::Field, ffi};

use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::{PyAny, PyObject, PyResult, Python};

use crate::{
    error::DaftResult,
    schema::SchemaRef,
    series::Series,
    table::Table,
    utils::arrow::{cast_array_for_daft_if_needed, cast_array_from_daft_if_needed},
};

pub type ArrayRef = Box<dyn Array>;

pub fn array_to_rust(arrow_array: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    arrow_array.call_method1(
        pyo3::intern!(arrow_array.py(), "_export_to_c"),
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).unwrap();
        let array = ffi::import_array_from_c(*array, field.data_type).unwrap();
        Ok(array)
    }
}

pub fn record_batches_to_table(
    py: Python,
    batches: &[&PyAny],
    schema: SchemaRef,
) -> PyResult<Table> {
    if batches.is_empty() {
        return Ok(Table::empty(Some(schema))?);
    }

    let names = schema.names();
    let num_batches = batches.len();
    // First extract all the arrays at once while holding the GIL
    let mut extracted_arrow_arrays: Vec<Vec<Box<dyn arrow2::array::Array>>> =
        Vec::with_capacity(num_batches);
    for rb in batches {
        let pycolumns = rb.getattr(pyo3::intern!(rb.py(), "columns"))?;
        let columns = pycolumns
            .downcast::<PyList>()?
            .into_iter()
            .map(array_to_rust)
            .collect::<PyResult<Vec<_>>>()?;
        if names.len() != columns.len() {
            return Err(PyValueError::new_err(format!("Error when converting Arrow Record Batches to Daft Table. Expected: {} columns, got: {}", names.len(), columns.len())));
        }
        extracted_arrow_arrays.push(columns);
    }
    // Now do the heavy lifting (casting and concats) without the GIL.
    py.allow_threads(|| {
        let mut tables: Vec<Table> = Vec::with_capacity(num_batches);
        for cols in extracted_arrow_arrays {
            let columns = cols
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let c = cast_array_for_daft_if_needed(c);
                    Series::try_from((names.get(i).unwrap().as_str(), c))
                })
                .collect::<DaftResult<Vec<_>>>()?;
            tables.push(Table::from_columns(columns)?)
        }
        Ok(Table::concat(
            tables.iter().collect::<Vec<&Table>>().as_slice(),
        )?)
    })
}

pub fn to_py_array(array: ArrayRef, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&Field::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let new_arr = fix_child_array_slice_offsets(array);
    let arrow_arr = Box::new(ffi::export_array_to_c(new_arr));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*arrow_arr;

    let array = pyarrow.getattr(pyo3::intern!(py, "Array"))?.call_method1(
        pyo3::intern!(py, "_import_from_c"),
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

pub fn table_to_record_batch(table: &Table, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(table.num_columns());
    let mut names: Vec<String> = Vec::with_capacity(table.num_columns());

    for i in 0..table.num_columns() {
        let s = table.get_column_by_index(i)?;
        let arrow_array = s.to_arrow();
        let arrow_array = cast_array_from_daft_if_needed(arrow_array.to_boxed());
        let py_array = to_py_array(arrow_array, py, pyarrow)?;
        arrays.push(py_array);
        names.push(s.name().to_string());
    }

    let record = pyarrow
        .getattr(pyo3::intern!(py, "RecordBatch"))?
        .call_method1(pyo3::intern!(py, "from_arrays"), (arrays, names.to_vec()))?;

    Ok(record.to_object(py))
}

fn fix_child_array_slice_offsets(array: ArrayRef) -> ArrayRef {
    /* Zero-copy slices of arrow2 struct/fixed-size list arrays are currently not correctly
    converted to pyarrow struct/fixed-size list arrays when going over the FFI boundary;
    this helper function ensures that such arrays' slice representation is changed to work
    around this bug.

    -- The Problem --

    Arrow2 represents struct array and fixed-size list array slices by slicing the valdity bitmap
    and its children arrays; these slices will eventually propagate down to the underlying data
    buffers. When converting to the Arrow C Data Interface struct, these offsets exist on the
    validity bitmap and its children arrays, AND is also lifted to be a top-level offset on the
    struct/fixed-size -list array. This means that the offset will be double-applied when imported
    into a pyarrow array, where both the top-level array offset will be applied as well as the child
    array offset.

    -- The Workaround --

    This helper ensures that such offsets are eliminated by ensuring that the underlying data
    buffers are truncated to the slice; note that this creates a copy of the underlying data,
    so FFI is not currently zero-copy for struct arrays or fixed-size list arrays. We accomplish
    this buffer truncation by doing an IPC roundtrip on the array, which should result in a single
    copy of the array's underlying data.
    */
    // TODO(Clark): Fix struct array and fixed-size list array slice FFI upstream in arrow2/pyarrow.
    // TODO(Clark): Only apply this workaround if a slice offset exists for this array.
    if ![
        arrow2::datatypes::PhysicalType::Struct,
        arrow2::datatypes::PhysicalType::FixedSizeList,
    ]
    .contains(&array.data_type().to_physical_type())
    {
        return array;
    }
    // Write the IPC representation to an in-memory buffer.
    // TODO(Clark): Preallocate the vector with the requisite capacity, based on the array size?
    let mut cursor = Cursor::new(Vec::new());
    let options = arrow2::io::ipc::write::WriteOptions { compression: None };
    let mut writer = arrow2::io::ipc::write::StreamWriter::new(&mut cursor, options);
    // Construct a single-column schema.
    let schema = arrow2::datatypes::Schema::from(vec![arrow2::datatypes::Field::new(
        "struct",
        array.data_type().clone(),
        false,
    )]);
    // Write the schema to the stream.
    writer.start(&schema, None).unwrap();
    // Write the array to the stream.
    let record = arrow2::chunk::Chunk::new(vec![array]);
    writer.write(&record, None).unwrap();
    writer.finish().unwrap();
    // Reset the cursor to the beginning of the stream.
    cursor.set_position(0);
    // Read back the array from the IPC stream.
    let stream_metadata = arrow2::io::ipc::read::read_stream_metadata(&mut cursor).unwrap();
    let mut reader = arrow2::io::ipc::read::StreamReader::new(cursor, stream_metadata, None);
    // There should only be a single chunk in the stream.
    let state = reader.next().unwrap();
    assert!(reader.next().is_none());
    // Stream should be finished from the reader's perspective.
    assert!(reader.is_finished());
    match state {
        Ok(arrow2::io::ipc::read::StreamState::Some(chunk)) => chunk.arrays()[0].clone(),
        _ => panic!("shouldn't be reached"),
    }
}
