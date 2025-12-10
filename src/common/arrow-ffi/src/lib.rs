use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "python")]
use arrow::ffi::FFI_ArrowSchema;
use daft_arrow::{
    array::Array,
    datatypes::{DataType as Arrow2DataType, Field},
};
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
    // this DOES not properly preserve metadata, so we need to update the fields with any extension types.
    let mut arr = daft_arrow::array::from_data(&data);

    let field = arrow_schema::Field::try_from(schema.as_ref()).unwrap();
    if let Some(ext_name) = field.metadata().get("ARROW:extension:name") {
        let metadata = field.metadata().get("ARROW:extension:metadata");
        // I (cory) believe this would fail if you had nested extension types.
        // I'm not sure we (or others) use nested extension types, or if this is even supported by arrow
        let dtype = Arrow2DataType::Extension(
            ext_name.to_string(),
            Box::new(field.data_type().clone().into()),
            metadata.map(|s| s.to_string()),
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
    let field = convert_field(field);
    let field = arrow::ffi::FFI_ArrowSchema::try_from(&field).map_err(|e| {
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
    let field = convert_field(field.clone());

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

fn convert_field(field: daft_arrow::datatypes::Field) -> arrow_schema::Field {
    use arrow_schema::{Field as ArrowField, UnionFields};

    let dtype = match field.data_type {
        Arrow2DataType::Null => arrow_schema::DataType::Null,
        Arrow2DataType::Boolean => arrow_schema::DataType::Boolean,
        Arrow2DataType::Int8 => arrow_schema::DataType::Int8,
        Arrow2DataType::Int16 => arrow_schema::DataType::Int16,
        Arrow2DataType::Int32 => arrow_schema::DataType::Int32,
        Arrow2DataType::Int64 => arrow_schema::DataType::Int64,
        Arrow2DataType::UInt8 => arrow_schema::DataType::UInt8,
        Arrow2DataType::UInt16 => arrow_schema::DataType::UInt16,
        Arrow2DataType::UInt32 => arrow_schema::DataType::UInt32,
        Arrow2DataType::UInt64 => arrow_schema::DataType::UInt64,
        Arrow2DataType::Float16 => arrow_schema::DataType::Float16,
        Arrow2DataType::Float32 => arrow_schema::DataType::Float32,
        Arrow2DataType::Float64 => arrow_schema::DataType::Float64,
        Arrow2DataType::Timestamp(unit, tz) => {
            arrow_schema::DataType::Timestamp(unit.into(), tz.map(Into::into))
        }
        Arrow2DataType::Date32 => arrow_schema::DataType::Date32,
        Arrow2DataType::Date64 => arrow_schema::DataType::Date64,
        Arrow2DataType::Time32(unit) => arrow_schema::DataType::Time32(unit.into()),
        Arrow2DataType::Time64(unit) => arrow_schema::DataType::Time64(unit.into()),
        Arrow2DataType::Duration(unit) => arrow_schema::DataType::Duration(unit.into()),
        Arrow2DataType::Interval(unit) => arrow_schema::DataType::Interval(unit.into()),
        Arrow2DataType::Binary => arrow_schema::DataType::Binary,
        Arrow2DataType::FixedSizeBinary(size) => arrow_schema::DataType::FixedSizeBinary(size as _),
        Arrow2DataType::LargeBinary => arrow_schema::DataType::LargeBinary,
        Arrow2DataType::Utf8 => arrow_schema::DataType::Utf8,
        Arrow2DataType::LargeUtf8 => arrow_schema::DataType::LargeUtf8,
        Arrow2DataType::List(f) => arrow_schema::DataType::List(Arc::new((*f).into())),
        Arrow2DataType::FixedSizeList(f, size) => {
            arrow_schema::DataType::FixedSizeList(Arc::new((*f).into()), size as _)
        }
        Arrow2DataType::LargeList(f) => arrow_schema::DataType::LargeList(Arc::new((*f).into())),
        Arrow2DataType::Struct(f) => {
            arrow_schema::DataType::Struct(f.into_iter().map(ArrowField::from).collect())
        }
        Arrow2DataType::Union(fields, Some(ids), mode) => {
            let ids = ids.into_iter().map(|x| x as _);
            let fields = fields.into_iter().map(ArrowField::from);
            arrow_schema::DataType::Union(UnionFields::new(ids, fields), mode.into())
        }
        Arrow2DataType::Union(fields, None, mode) => {
            let ids = 0..fields.len() as i8;
            let fields = fields.into_iter().map(ArrowField::from);
            arrow_schema::DataType::Union(UnionFields::new(ids, fields), mode.into())
        }
        Arrow2DataType::Map(f, ordered) => {
            arrow_schema::DataType::Map(Arc::new((*f).into()), ordered)
        }
        Arrow2DataType::Dictionary(key, value, _) => arrow_schema::DataType::Dictionary(
            Box::new(Arrow2DataType::from(key).into()),
            Box::new((*value).into()),
        ),
        Arrow2DataType::Decimal(precision, scale) => {
            arrow_schema::DataType::Decimal128(precision as _, scale as _)
        }
        Arrow2DataType::Decimal256(precision, scale) => {
            arrow_schema::DataType::Decimal256(precision as _, scale as _)
        }
        Arrow2DataType::Extension(name, d, metadata) => {
            let mut metadata_map = HashMap::new();
            metadata_map.insert("ARROW:extension:name".to_string(), name);
            if let Some(metadata) = metadata {
                metadata_map.insert("ARROW:extension:metadata".to_string(), metadata);
            }
            return convert_field(Field::new(field.name, *d, true)).with_metadata(metadata_map);
        }
    };
    arrow_schema::Field::new(field.name, dtype, true)
}

#[cfg(feature = "python")]
pub fn dtype_to_py<'py>(
    py: Python<'py>,
    dtype: &daft_arrow::datatypes::DataType,
    pyarrow: Bound<'py, PyModule>,
) -> PyResult<Bound<'py, PyAny>> {
    let field = daft_arrow::datatypes::Field::new("", dtype.clone(), true);
    let field = convert_field(field);

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
