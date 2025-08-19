use chrono::{DateTime, TimeZone};
use common_arrow_ffi as ffi;
use common_error::DaftError;
use common_ndarray::NdArray;
use daft_schema::prelude::TimeUnit;
use indexmap::IndexMap;
use pyo3::{
    intern,
    prelude::*,
    types::{PyDict, PyList, PyNone},
    IntoPyObjectExt,
};

use super::Literal;
use crate::{
    lit::DaftFile,
    python::PySeries,
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
                .ok_or_else(||
                    DaftError::ValueError(format!("Overflow when constructing timestamp from value with time unit {tu}: {val}"))
                )?
                .naive_utc();

                match tz {
                    None => naive_dt.into_bound_py_any(py),
                    Some(tz_str)
                        if let Ok(fixed_offset) =
                            arrow2::temporal_conversions::parse_offset(&tz_str) =>
                    {
                        fixed_offset
                            .from_utc_datetime(&naive_dt)
                            .into_bound_py_any(py)
                    }
                    Some(tz_str)
                        if let Ok(tz) = arrow2::temporal_conversions::parse_offset_tz(&tz_str) =>
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
                .map(|(f, v)| (f.name, v))
                .collect::<IndexMap<_, _>>()
                .into_bound_py_any(py),
            Self::File(f) => {
                let py_file = py
                    .import(intern!(py, "daft.file"))?
                    .getattr(intern!(py, "File"))?;

                match f {
                    DaftFile::Data(data) => {
                        py_file.getattr(intern!(py, "_from_bytes"))?.call1((data,))
                    }
                    DaftFile::Reference(path) => {
                        py_file.getattr(intern!(py, "_from_path"))?.call1((path,))
                    }
                }
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
                ffi::to_py_array(py, data.to_arrow(), &pyarrow)?
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
                let values_arr = ffi::to_py_array(py, values.to_arrow(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))?;
                let indices_arr = ffi::to_py_array(py, indices.to_arrow(), &pyarrow)?
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
                ffi::to_py_array(py, series.to_arrow(), &pyarrow)?
                    .call_method1(pyo3::intern!(py, "to_numpy"), (false,))
            }
            Self::Image(image) => {
                let img_arr: Box<dyn NdArray> = image.into_ndarray();
                Ok(img_arr.into_py(py))
            }
            Self::Extension(series) => {
                debug_assert_eq!(
                    series.len(),
                    1,
                    "Expected extension literal to have length 1"
                );

                let pyarrow = py.import(pyo3::intern!(py, "pyarrow"))?;

                let arrow_array = series.to_arrow();
                let arrow_array = cast_array_from_daft_if_needed(arrow_array);

                ffi::to_py_array(py, arrow_array, &pyarrow)?
                    .call_method0(pyo3::intern!(py, "to_pylist"))?
                    .get_item(0)
            }
        }
    }
}
