use std::{collections::HashMap, str::FromStr, sync::Arc};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone};
use common_arrow_ffi as ffi;
use common_error::DaftError;
use common_ndarray::NumpyArray;
use daft_schema::{
    dtype::DataType,
    prelude::{ImageMode, TimeUnit},
    python::{PyDataType, PyTimeUnit},
};
use indexmap::{IndexMap, indexmap};
use pyo3::{
    IntoPyObjectExt, PyTypeCheck,
    exceptions::PyValueError,
    intern,
    prelude::*,
    pybacked::PyBackedStr,
    types::{
        PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyNone,
        PyString, PyTime, PyTuple,
    },
};

use super::Literal;
use crate::{
    file::FileReference,
    python::PySeries,
    series::Series,
    utils::{arrow::cast_array_from_daft_if_needed, display::display_decimal128},
};

/// All Daft to Python type conversion logic should go through this implementation.
///
/// The behavior here should be documented in `docs/api/datatypes.md`
impl<'py> IntoPyObject<'py> for Literal {
    type Target = PyAny;

    type Output = Bound<'py, Self::Target>;

    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        fn div_rem(l: i64, r: i64) -> (i64, i64) {
            (l / r, l % r)
        }

        match self {
            Self::Null => Ok(PyNone::get(py).to_owned().into_any()),
            Self::Boolean(val) => val.into_bound_py_any(py),
            Self::Utf8(val) => val.into_bound_py_any(py),
            Self::Binary(val) => val.into_bound_py_any(py),
            Self::Int8(val) => val.into_bound_py_any(py),
            Self::UInt8(val) => val.into_bound_py_any(py),
            Self::Int16(val) => val.into_bound_py_any(py),
            Self::UInt16(val) => val.into_bound_py_any(py),
            Self::Int32(val) => val.into_bound_py_any(py),
            Self::UInt32(val) => val.into_bound_py_any(py),
            Self::Int64(val) => val.into_bound_py_any(py),
            Self::UInt64(val) => val.into_bound_py_any(py),
            Self::Timestamp(val, tu, tz) => {
                // TODO: store chrono DateTime in timestamp literal directly
                let naive_dt = match tu {
                    TimeUnit::Nanoseconds => Some(DateTime::from_timestamp_nanos(val)),
                    TimeUnit::Microseconds => DateTime::from_timestamp_micros(val),
                    TimeUnit::Milliseconds => DateTime::from_timestamp_millis(val),
                    TimeUnit::Seconds => DateTime::from_timestamp(val, 0),
                }
                .ok_or_else(|| {
                    DaftError::ValueError(format!(
                        "Overflow when constructing timestamp from value with time unit {tu}: {val}"
                    ))
                })?
                .naive_utc();

                match tz {
                    None => naive_dt.into_bound_py_any(py),
                    Some(tz_str)
                        if let Ok(fixed_offset) = daft_schema::time_unit::parse_offset(&tz_str) =>
                    {
                        fixed_offset
                            .from_utc_datetime(&naive_dt)
                            .into_bound_py_any(py)
                    }
                    Some(tz_str)
                        if let Ok(tz) = daft_schema::time_unit::parse_offset_tz(&tz_str) =>
                    {
                        tz.from_utc_datetime(&naive_dt).into_bound_py_any(py)
                    }
                    Some(tz_str) => Err(DaftError::ValueError(format!(
                        "Failed to parse timezone string: {tz_str}"
                    ))
                    .into()),
                }
            }
            Self::Date(val) => DateTime::from_timestamp((val as i64) * 24 * 60 * 60, 0)
                .expect("chrono::DateTime from i32 date should not overflow")
                .naive_utc()
                .date()
                .into_bound_py_any(py),
            Self::Time(val, time_unit) => {
                let (h, m, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (h, m, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (h, rem) = div_rem(val, 60 * 60 * 1_000);
                        let (m, rem) = div_rem(rem, 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (h, m, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (h, rem) = div_rem(val, 60 * 60);
                        let (m, s) = div_rem(rem, 60);
                        (h, m, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "time"))?
                    .call1((h, m, s, us))
            }
            Self::Duration(val, time_unit) => {
                let (d, s, us) = match time_unit {
                    TimeUnit::Nanoseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000_000);
                        let (s, rem) = div_rem(rem, 1_000_000_000);
                        let us = rem / 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Microseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000_000);
                        let (s, us) = div_rem(rem, 1_000_000);
                        (d, s, us)
                    }
                    TimeUnit::Milliseconds => {
                        let (d, rem) = div_rem(val, 24 * 60 * 60 * 1_000);
                        let (s, ms) = div_rem(rem, 1_000);
                        let us = ms * 1_000;
                        (d, s, us)
                    }
                    TimeUnit::Seconds => {
                        let (d, s) = div_rem(val, 24 * 60 * 60);
                        (d, s, 0)
                    }
                };

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "timedelta"))?
                    .call1((d, s, us))
            }
            Self::Interval(_) => {
                Err(DaftError::NotImplemented("Interval literal to Python".to_string()).into())
            }
            Self::Float32(val) => val.into_bound_py_any(py),
            Self::Float64(val) => val.into_bound_py_any(py),
            Self::Decimal(val, p, s) => py
                .import(intern!(py, "decimal"))?
                .getattr(intern!(py, "Decimal"))?
                .call1((display_decimal128(val, p, s),)),
            Self::List(series) => Ok(PySeries { series }.to_pylist(py)?.into_any()),
            Self::Python(val) => val.0.as_ref().into_bound_py_any(py),
            Self::Struct(entries) => entries
                .into_iter()
                .filter(|(k, v)| !(k.is_empty() && *v == Self::Null))
                .collect::<IndexMap<_, _>>()
                .into_bound_py_any(py),
            Self::File(f) => {
                let file_class = match f.media_type {
                    daft_schema::media_type::MediaType::Unknown => intern!(py, "File"),
                    daft_schema::media_type::MediaType::Video => intern!(py, "VideoFile"),
                    daft_schema::media_type::MediaType::Audio => intern!(py, "AudioFile"),
                };

                let pytuple = f.into_bound_py_any(py)?;
                let py_file = py
                    .import(intern!(py, "daft.daft"))?
                    .getattr(intern!(py, "PyFileReference"))?;
                let res = py_file.call_method1(pyo3::intern!(py, "_from_tuple"), (pytuple,))?;
                let py_file = py.import(intern!(py, "daft.file"))?.getattr(file_class)?;
                py_file.call_method1(pyo3::intern!(py, "_from_file_reference"), (res,))
            }
            Self::Map { keys, values } => {
                assert_eq!(
                    keys.len(),
                    values.len(),
                    "Key and value counts should be equal in map literal"
                );

                Ok(PyList::new(py, keys.to_literals().zip(values.to_literals()))?.into_any())
            }
            Self::Tensor { data, shape } => {
                let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
                ffi::to_py_array(py, data.to_arrow2(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))?
                    .call_method1(pyo3::intern!(py, "reshape"), (shape,))
            }
            Self::SparseTensor {
                values,
                indices,
                shape,
                ..
            } => {
                let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
                let values_arr = ffi::to_py_array(py, values.to_arrow2(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))?;
                let indices_arr = ffi::to_py_array(py, indices.to_arrow2(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))?;

                let seq = (
                    ("values", values_arr),
                    ("indices", indices_arr),
                    ("shape", shape),
                )
                    .into_bound_py_any(py)?;

                Ok(PyDict::from_sequence(&seq)?.into_any())
            }
            Self::Embedding(series) => {
                let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;
                ffi::to_py_array(py, series.to_arrow2(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))
            }
            Self::Image(image) => {
                let img_arr = image.into_ndarray();
                NumpyArray::from_ndarray(&img_arr, py).into_pyobject(py)
            }
            Self::Extension(series) => {
                debug_assert_eq!(
                    series.len(),
                    1,
                    "Expected extension literal to have length 1"
                );

                let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;

                let arrow_array = series.to_arrow2();
                let arrow_array = cast_array_from_daft_if_needed(arrow_array);

                ffi::to_py_array(py, arrow_array, &pyarrow)?
                    .call_method0(pyo3::intern!(py, "to_pylist"))?
                    .get_item(0)
            }
        }
    }
}

impl Literal {
    /// Convert a Python object into a Daft Literal.
    ///
    /// NOTE: Make sure this matches the logic in `DataType.infer_from_type` in Python
    pub fn from_pyobj(ob: &Bound<PyAny>, dtype: Option<&DataType>) -> PyResult<Self> {
        fn isinstance_impl(
            ob: &Bound<PyAny>,
            module: &Bound<PyString>,
            ty: &Bound<PyString>,
        ) -> PyResult<bool> {
            let ty_obj = ob.py().import(module)?.getattr(ty)?;
            ob.is_instance(&ty_obj)
        }

        fn get_numpy_scalar<'py>(ob: &'py Bound<PyAny>) -> PyResult<Bound<'py, PyAny>> {
            ob.call_method0(intern!(ob.py(), "item"))
        }

        macro_rules! isinstance {
            ($ob:expr, $module:expr, $ty:expr) => {
                isinstance_impl($ob, intern!($ob.py(), $module), intern!($ob.py(), $ty))
                    .unwrap_or(false)
            };
        }

        // keep the code here minimal to clearly show the mapping from Python type to Daft type
        let lit = if matches!(dtype, Some(DataType::Null)) {
            // check if we want null dtype because anything can technically be casted to null
            Self::Null
        } else if ob.is_none() {
            Self::Null
        } else if matches!(dtype, Some(DataType::Python)) {
            // check if we want python dtype first to avoid any conversions
            Self::Python(Arc::new(ob.clone().unbind()).into())
        } else if PyBool::type_check(ob) {
            Self::Boolean(ob.extract()?)
        } else if PyString::type_check(ob) {
            Self::Utf8(ob.extract()?)
        } else if PyBytes::type_check(ob) {
            Self::Binary(ob.extract()?)
        } else if PyInt::type_check(ob) {
            if ob.le(i64::MAX)? {
                Self::Int64(ob.extract()?)
            } else {
                Self::UInt64(ob.extract()?)
            }
        } else if PyFloat::type_check(ob) {
            Self::Float64(ob.extract()?)
        } else if PyDateTime::type_check(ob) {
            pydatetime_to_timestamp_lit(ob)?
        } else if PyDate::type_check(ob) {
            pydate_to_date_lit(ob)?
        } else if PyTime::type_check(ob) {
            pytime_to_time_lit(ob)?
        } else if PyDelta::type_check(ob) {
            pydelta_to_duration_lit(ob)?
        } else if PyList::type_check(ob) {
            pylist_to_list_lit(ob, dtype)?
        } else if PyDict::type_check(ob) {
            let dict = ob.cast::<PyDict>()?;

            // if the data type was explicitly specified, respect that
            // otherwise, infer based on key types
            match dtype {
                Some(DataType::Struct(_)) => pydict_to_struct_lit(dict, dtype)?,
                Some(DataType::Map { .. }) => pydict_to_map_lit(dict, dtype)?,
                _ => {
                    let all_string_keys = dict.iter().all(|(k, _)| k.is_instance_of::<PyString>());

                    if all_string_keys {
                        pydict_to_struct_lit(dict, dtype)?
                    } else {
                        pydict_to_map_lit(dict, dtype)?
                    }
                }
            }
        } else if PyTuple::type_check(ob) {
            pytuple_to_struct_lit(ob, dtype)?
        } else if isinstance!(ob, "pydantic", "BaseModel") {
            pydantic_model_to_struct_lit(ob, dtype)?
        } else if isinstance!(ob, "decimal", "Decimal") {
            pydecimal_to_decimal_lit(ob)?
        } else if isinstance!(ob, "numpy", "ndarray")
            || isinstance!(ob, "torch", "Tensor")
            || isinstance!(ob, "tensorflow", "Tensor")
            || isinstance!(ob, "jax", "Array")
        {
            numpy_array_like_to_tensor_lit(ob)?
        } else if isinstance!(ob, "cupy", "ndarray") {
            cupy_array_to_tensor_lit(ob)?
        } else if isinstance!(ob, "numpy", "bool_") {
            Self::Boolean(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "int8") {
            Self::Int8(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "uint8") {
            Self::UInt8(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "int16") {
            Self::Int16(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "uint16") {
            Self::UInt16(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "int32") {
            Self::Int32(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "uint32") {
            Self::UInt32(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "int64") {
            Self::Int64(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "uint64") {
            Self::UInt64(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "float32") {
            Self::Float32(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "float64") {
            Self::Float64(get_numpy_scalar(ob)?.extract()?)
        } else if isinstance!(ob, "numpy", "datetime64") {
            numpy_datetime64_to_date_or_timestamp_lit(ob)?
        } else if isinstance!(ob, "pandas", "Series") {
            pandas_series_to_list_lit(ob)?
        } else if isinstance!(ob, "PIL.Image", "Image") {
            pil_image_to_image_or_py_lit(ob)?
        } else if isinstance!(ob, "daft.series", "Series") {
            daft_series_to_list_lit(ob)?
        } else if isinstance!(ob, "daft.file", "File") {
            daft_file_to_file_lit(ob)?
        } else {
            Self::Python(Arc::new(ob.clone().unbind()).into())
        };

        // do a cast at the end if type is specified
        let lit = if let Some(dtype) = dtype {
            if matches!(lit, Self::Python(_)) && !dtype.is_python() {
                // we aren't able to convert this into anything native, so casting would just cause infinite recursion
                return Err(DaftError::TypeError(format!(
                    "Python object could not be casted to `{dtype}` data type: {ob}"
                ))
                .into());
            }

            lit.cast(dtype)?
        } else {
            lit
        };

        Ok(lit)
    }
}

fn pydatetime_to_timestamp_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    /// Used for implementing extraction from the pytz library
    #[derive(Clone)]
    struct PyTz(chrono_tz::Tz);

    impl<'py> FromPyObject<'_, 'py> for PyTz {
        type Error = PyErr;

        fn extract(ob: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
            chrono_tz::Tz::from_str(
                &ob.getattr(intern!(ob.py(), "zone"))?
                    .extract::<PyBackedStr>()?,
            )
            .map(Self)
            .map_err(|e| PyValueError::new_err(e.to_string()))
        }
    }

    impl TimeZone for PyTz {
        type Offset = <chrono_tz::Tz as TimeZone>::Offset;

        fn from_offset(offset: &Self::Offset) -> Self {
            Self(chrono_tz::Tz::from_offset(offset))
        }

        fn offset_from_local_date(
            &self,
            local: &NaiveDate,
        ) -> chrono::MappedLocalTime<Self::Offset> {
            self.0.offset_from_local_date(local)
        }

        fn offset_from_local_datetime(
            &self,
            local: &NaiveDateTime,
        ) -> chrono::MappedLocalTime<Self::Offset> {
            self.0.offset_from_local_datetime(local)
        }

        fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
            self.0.offset_from_utc_date(utc)
        }

        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
            self.0.offset_from_utc_datetime(utc)
        }
    }

    if let Ok(dt) = ob.extract::<NaiveDateTime>() {
        let ts = dt.and_utc().timestamp_micros();
        Ok(Literal::Timestamp(ts, TimeUnit::Microseconds, None))
    } else if let Ok(dt) = ob.extract::<DateTime<chrono_tz::Tz>>() {
        let ts = dt.timestamp_micros();
        Ok(Literal::Timestamp(
            ts,
            TimeUnit::Microseconds,
            Some(dt.timezone().to_string()),
        ))
    } else if let Ok(dt) = ob.extract::<DateTime<PyTz>>() {
        let ts = dt.timestamp_micros();
        Ok(Literal::Timestamp(
            ts,
            TimeUnit::Microseconds,
            Some(dt.timezone().0.to_string()),
        ))
    } else if let Ok(dt) = ob.extract::<DateTime<chrono::FixedOffset>>() {
        let ts = dt.timestamp_micros();
        Ok(Literal::Timestamp(
            ts,
            TimeUnit::Microseconds,
            Some(dt.timezone().to_string()),
        ))
    } else {
        Err(DaftError::ValueError(format!(
            "Failed to convert Python object to timestamp literal: {ob}"
        ))
        .into())
    }
}

fn pydate_to_date_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let date = ob.extract::<NaiveDate>()?;
    let days_since_epoch = (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
    Ok(Literal::Date(days_since_epoch))
}

fn pytime_to_time_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let time = ob.extract::<NaiveTime>()?;
    let microseconds = (time - NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .num_microseconds()
        .expect("number of microseconds in a day should never overflow i64");
    Ok(Literal::Time(microseconds, TimeUnit::Microseconds))
}

fn pydelta_to_duration_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let duration = ob.extract::<TimeDelta>()?;
    let microseconds = duration.num_microseconds().ok_or_else(|| DaftError::ValueError(format!("Integer overflow when trying to convert `datetime.timedelta` to int64 number of microseconds: {ob}")))?;
    Ok(Literal::Duration(microseconds, TimeUnit::Microseconds))
}

fn pylist_to_list_lit(ob: &Bound<PyAny>, dtype: Option<&DataType>) -> PyResult<Literal> {
    let list = ob.cast::<PyList>()?;
    let child_dtype = dtype.and_then(|t| match t {
        DataType::List(child) | DataType::FixedSizeList(child, _) => {
            Some(child.as_ref().clone().into())
        }
        _ => None,
    });
    let series = PySeries::from_pylist(list, None, child_dtype)?.series;
    Ok(Literal::List(series))
}

fn pydict_to_struct_lit(dict: &Bound<PyDict>, dtype: Option<&DataType>) -> PyResult<Literal> {
    let field_dtypes = if let Some(DataType::Struct(fields)) = dtype {
        fields.iter().map(|f| (&f.name, &f.dtype)).collect()
    } else {
        HashMap::new()
    };
    let field_mapping = dict.iter().map(|(k, v)| {
        let field_name = k.extract::<String>().map_err(|_| DaftError::TypeError(format!("Expected all dict keys when converting into Daft struct to be string, found: {k}")))?;
        let field_dtype = field_dtypes.get(&field_name).copied();
        let field_value = Literal::from_pyobj(&v, field_dtype)?;

        Ok((field_name, field_value))
    }).collect::<PyResult<IndexMap<_, _>>>()?;

    if field_mapping.is_empty() {
        Ok(Literal::Struct(indexmap! {String::new() => Literal::Null}))
    } else {
        Ok(Literal::Struct(field_mapping))
    }
}

fn pydict_to_map_lit(dict: &Bound<PyDict>, dtype: Option<&DataType>) -> PyResult<Literal> {
    let (key_dtype, value_dtype) = if let Some(DataType::Map { key, value }) = dtype {
        (
            Some(key.as_ref().clone().into()),
            Some(value.as_ref().clone().into()),
        )
    } else {
        (None, None)
    };

    let keys = PySeries::from_pylist(&dict.keys(), None, key_dtype)?.series;
    let values = PySeries::from_pylist(&dict.values(), None, value_dtype)?.series;

    Ok(Literal::Map { keys, values })
}

fn pytuple_to_struct_lit(ob: &Bound<PyAny>, dtype: Option<&DataType>) -> PyResult<Literal> {
    let tuple = ob.cast::<PyTuple>()?;

    let field_mapping: IndexMap<_, _> = if let Some(DataType::Struct(fields)) = dtype
        && tuple.len() == fields.len()
    {
        tuple
            .iter()
            .zip(fields)
            .map(|(v, f)| {
                let field_value = Literal::from_pyobj(&v, Some(&f.dtype))?;

                Ok((f.name.clone(), field_value))
            })
            .collect::<PyResult<_>>()
    } else {
        tuple
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let field_name = format!("_{i}");
                let field_value = Literal::from_pyobj(&v, None)?;

                Ok((field_name, field_value))
            })
            .collect::<PyResult<_>>()
    }?;

    if field_mapping.is_empty() {
        Ok(Literal::Struct(indexmap! {String::new() => Literal::Null}))
    } else {
        Ok(Literal::Struct(field_mapping))
    }
}

fn pydantic_model_to_struct_lit(ob: &Bound<PyAny>, dtype: Option<&DataType>) -> PyResult<Literal> {
    let py = ob.py();

    // get richer dtype info from object type if dtype not specified
    let dtype = if let Some(dtype) = dtype {
        dtype.clone()
    } else {
        let datatype_cls = py
            .import(intern!(py, "daft.datatype"))?
            .getattr(intern!(py, "DataType"))?;
        let py_dtype =
            datatype_cls.call_method1(intern!(py, "infer_from_type"), (ob.get_type(),))?;
        py_dtype
            .getattr(intern!(py, "_dtype"))?
            .extract::<PyDataType>()?
            .dtype
    };

    let dict = ob.call_method0(intern!(py, "model_dump"))?;
    let dict = dict.cast::<PyDict>()?;

    pydict_to_struct_lit(dict, Some(&dtype))
}

fn pydecimal_to_decimal_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    // call `format(ob, 'f')` to force decimal notation, this handles scientific notation.
    let py = ob.py();
    let format_func = py.import("builtins")?.getattr("format")?;
    let pystring = format_func.call1((ob, "f"))?.str()?;
    let decimal_str = pystring.to_cow()?;

    // just always use max precision. we will cast if user specifies a different precision
    let precision: u8 = 38;

    let (scale, val) = if let Some((integral, fractional)) = decimal_str.split_once('.') {
        let scale = fractional.len().try_into().map_err(|_| {
            DaftError::ValueError(format!(
                "Failed to parse `decimal.Decimal` to Daft decimal type, scale overflows 8-bit integer: {ob}"
            ))
        })?;
        let val = format!("{integral}{fractional}").parse().map_err(|_| {
            DaftError::ValueError(format!(
                "Failed to parse `decimal.Decimal` to Daft decimal type, value overflows 128-bit integer: {ob}"
            ))
        })?;

        (scale, val)
    } else {
        // we do not support negative scale yet, just make the scale zero for numbers without fractional component
        let scale = 0;
        let val = decimal_str.parse().map_err(|_| {
            DaftError::ValueError(format!(
                "Failed to parse `decimal.Decimal` to Daft decimal type, value overflows 128-bit integer: {ob}"
            ))
        })?;

        (scale, val)
    };

    Ok(Literal::Decimal(val, precision, scale))
}

fn numpy_array_like_to_tensor_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let py = ob.py();
    let np_asarray = py
        .import(intern!(py, "numpy"))?
        .getattr(intern!(py, "asarray"))?;
    let ob = np_asarray.call1((ob,))?;

    let arr = if let Ok(arr) = ob.extract::<NumpyArray>() {
        arr
    } else {
        // if we do not support the element type, fall back to Python.
        // Series::from_ndarray_flattened will then call Literal::from_pyobj to try to convert each element.
        let object_array = ob
            .call_method1(intern!(py, "astype"), (intern!(py, "O"),))?
            .extract()?;
        NumpyArray::Py(object_array)
    };

    let arr = arr.to_ndarray();
    let shape = arr.shape().iter().map(|dim| *dim as _).collect();

    let data = Series::from_ndarray_flattened(arr);

    Ok(Literal::Tensor { data, shape })
}

fn cupy_array_to_tensor_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let numpy_array = ob.call_method0(intern!(ob.py(), "get"))?;
    numpy_array_like_to_tensor_lit(&numpy_array)
}

fn pandas_series_to_list_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let py = ob.py();

    let series = py
        .import(intern!(py, "daft.series"))?
        .getattr(intern!(py, "Series"))?
        .call_method1(intern!(py, "from_pandas"), (ob,))?
        .getattr(intern!(py, "_series"))?
        .extract::<PySeries>()?
        .series;

    Ok(Literal::List(series))
}

fn numpy_datetime64_to_date_or_timestamp_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let py = ob.py();

    let (val, tu) = py
        .import(intern!(py, "daft.utils"))?
        .getattr(intern!(py, "np_datetime64_to_timestamp"))?
        .call1((ob,))?
        .extract::<(i64, Option<PyTimeUnit>)>()?;

    if let Some(tu) = tu {
        let tu = tu.timeunit;

        Ok(Literal::Timestamp(val, tu, None))
    } else {
        let val = val.try_into().map_err(|_| {
            DaftError::ValueError(format!(
                "Overflow when converting `np.datetime64` to 32-bit date: {ob}"
            ))
        })?;
        Ok(Literal::Date(val))
    }
}

fn pil_image_to_image_or_py_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let mode = ob.getattr(intern!(ob.py(), "mode"))?.extract::<String>()?;
    if ImageMode::from_pil_mode_str(&mode).is_err() {
        // if it's an unsupported image mode, fall back to Python
        return Ok(Literal::Python(Arc::new(ob.clone().unbind()).into()));
    }

    Ok(Literal::Image(ob.extract()?))
}

fn daft_series_to_list_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    Ok(Literal::List(
        ob.getattr(intern!(ob.py(), "_series"))?
            .extract::<PySeries>()?
            .series,
    ))
}

fn daft_file_to_file_lit(ob: &Bound<PyAny>) -> PyResult<Literal> {
    let py = ob.py();
    let file: FileReference = ob
        .getattr(intern!(py, "_inner"))?
        .call_method0(intern!(py, "_get_file"))?
        .extract()?;
    Ok(Literal::File(file))
}
