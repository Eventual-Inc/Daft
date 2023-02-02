use arrow2::compute::cast;
use arrow2::{array::Array, datatypes::Field, ffi};

use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::{PyAny, PyObject, PyResult, Python};

use crate::error::DaftResult;
use crate::series::Series;
use crate::table::Table;

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
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).unwrap();
        let array = ffi::import_array_from_c(*array, field.data_type).unwrap();
        Ok(array)
    }
}

pub fn record_batches_to_table(batches: &[&PyAny]) -> PyResult<Table> {
    if batches.is_empty() {
        return Err(PyValueError::new_err(
            "received an empty list of arrow record batches. Can not infer a schema.",
        ));
    }
    if batches.len() > 1 {
        return Err(PyValueError::new_err(
            "we can only handle a single record batch right now",
        ));
    }

    let schema = batches.get(0).unwrap().getattr("schema").unwrap();
    let names = schema.getattr("names")?.extract::<Vec<String>>()?;
    let rb = *batches.get(0).unwrap();
    let columns: DaftResult<Vec<Series>> = (0..names.len())
        .map(|i| {
            let array = rb.call_method1("column", (i,)).unwrap();
            let arr = array_to_rust(array).unwrap();
            let arr = match arr.data_type() {
                arrow2::datatypes::DataType::Utf8 => {
                    cast::utf8_to_large_utf8(arr.as_ref().as_any().downcast_ref().unwrap()).boxed()
                }
                arrow2::datatypes::DataType::Binary => cast::binary_to_large_binary(
                    arr.as_ref().as_any().downcast_ref().unwrap(),
                    arrow2::datatypes::DataType::LargeBinary,
                )
                .boxed(),
                _ => arr,
            };

            Series::try_from((names.get(i).unwrap().as_str(), arr))
        })
        .collect();

    Ok(Table::from_columns(columns?)?)
}

pub fn to_py_array(array: ArrayRef, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&Field::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

pub fn table_to_record_batch(table: &Table, py: Python, pyarrow: &PyModule) -> PyResult<PyObject> {
    let mut arrays = Vec::with_capacity(table.num_columns());
    let mut names: Vec<String> = Vec::with_capacity(table.num_columns());

    for i in 0..table.num_columns() {
        let s = table.get_column_by_index(i)?;
        let arrow_array = s.array().data();
        let py_array = to_py_array(arrow_array.to_boxed(), py, pyarrow)?;
        arrays.push(py_array);
        names.push(s.name().to_string());
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names.to_vec()))?;

    Ok(record.to_object(py))
}
