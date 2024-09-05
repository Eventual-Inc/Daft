#[cfg(feature = "python")]
use pyo3::prelude::*;

use common_error::DaftError;
use daft_core::prelude::*;

make_udf_function! {
    name: "date",
    to_field: (input, schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::Date))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to date to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_date()
    }
}

make_udf_function! {
    name: "day",
    to_field: (input, schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::UInt32))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to day to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),

        }
    },
    evaluate: (input) {
        input.dt_day()
    }
}

make_udf_function! {
    name: "day_of_week",
    to_field: (input, schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::UInt32))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to day to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_day_of_week()
    }
}

make_udf_function! {
    name: "hour",
    to_field: (input, schema) {
        match input.to_field(schema) {
                Ok(field) if field.dtype.is_temporal() => {
                    Ok(Field::new(field.name, DataType::UInt32))
                }
                Ok(field) => Err(DaftError::TypeError(format!(
                    "Expected input to hour to be temporal, got {}",
                    field.dtype
                ))),
                Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_hour()
    }
}

make_udf_function! {
    name: "minute",
    to_field: (input,schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::UInt32))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to minute to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_minute()
    }
}

make_udf_function! {
    name: "month",
    to_field: (input,schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::UInt32))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to month to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_month()
    }
}
make_udf_function! {
    name: "second",
    to_field: (input,schema) {
        match input.to_field(schema) {
                Ok(field) if field.dtype.is_temporal() => {
                    Ok(Field::new(field.name, DataType::UInt32))
                }
                Ok(field) => Err(DaftError::TypeError(format!(
                    "Expected input to second to be temporal, got {}",
                    field.dtype
                ))),
                Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_second()
    }
}
make_udf_function! {
    name: "time",
    to_field: (input,schema) {
        match input.to_field(schema) {
            Ok(field) => match field.dtype {
                DataType::Time(_) => Ok(field),
                DataType::Timestamp(tu, _) => {
                    let tu = match tu {
                        TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                        _ => TimeUnit::Microseconds,
                    };
                    Ok(Field::new(field.name, DataType::Time(tu)))
                }
                _ => Err(DaftError::TypeError(format!(
                    "Expected input to time to be time or timestamp, got {}",
                    field.dtype
                ))),
            },
            Err(e) => Err(e),

        }
    },
    evaluate: (input) {
        input.dt_time()
    }
}
make_udf_function! {
    name: "year",
    to_field: (input,schema) {
        match input.to_field(schema) {
            Ok(field) if field.dtype.is_temporal() => {
                Ok(Field::new(field.name, DataType::UInt32))
            }
            Ok(field) => Err(DaftError::TypeError(format!(
                "Expected input to year to be temporal, got {}",
                field.dtype
            ))),
            Err(e) => Err(e),
        }
    },
    evaluate: (input) {
        input.dt_year()
    }
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(py_date))?;
    parent.add_wrapped(wrap_pyfunction!(py_day))?;
    parent.add_wrapped(wrap_pyfunction!(py_day_of_week))?;
    parent.add_wrapped(wrap_pyfunction!(py_hour))?;
    parent.add_wrapped(wrap_pyfunction!(py_minute))?;
    parent.add_wrapped(wrap_pyfunction!(py_month))?;
    parent.add_wrapped(wrap_pyfunction!(py_second))?;
    parent.add_wrapped(wrap_pyfunction!(py_time))?;
    parent.add_wrapped(wrap_pyfunction!(py_year))?;
    Ok(())
}
