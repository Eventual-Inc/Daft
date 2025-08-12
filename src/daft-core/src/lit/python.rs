use common_error::DaftError;
use daft_schema::prelude::TimeUnit;
use indexmap::IndexMap;
use pyo3::{intern, prelude::*, types::PyNone, IntoPyObjectExt};

use super::Literal;
use crate::{lit::DaftFile, python::PySeries, utils::display::display_decimal128};

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
            Self::Timestamp(val, time_unit, tz) => {
                let ts = (val as f64) / (time_unit.to_scale_factor() as f64);

                py.import(intern!(py, "datetime"))?
                    .getattr(intern!(py, "datetime"))?
                    .call_method1(intern!(py, "fromtimestamp"), (ts, tz))
            }
            Self::Date(val) => py
                .import(intern!(py, "datetime"))?
                .getattr(intern!(py, "date"))?
                .call_method1(intern!(py, "fromtimestamp"), (val,)),
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
            Self::List(series) => py
                .import(intern!(py, "daft.series"))?
                .getattr(intern!(py, "Series"))?
                .getattr(intern!(py, "_from_pyseries"))?
                .call1((PySeries { series },)),
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
        }
    }
}
