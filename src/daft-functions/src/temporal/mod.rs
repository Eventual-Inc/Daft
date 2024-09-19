pub mod truncate;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, TimeUnit},
    series::Series,
};
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
#[cfg(feature = "python")]
use pyo3::{prelude::*, pyfunction, PyResult};
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction_bound!(py_dt_date::dt_date, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_day::dt_day, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(
        py_dt_day_of_week::dt_day_of_week,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_hour::dt_hour, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_minute::dt_minute, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_month::dt_month, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_second::dt_second, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_time::dt_time, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(py_dt_year::dt_year, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(truncate::py_dt_truncate, parent)?)?;
    Ok(())
}

macro_rules! impl_temporal {
    ($name_str:expr, $name:ident, $dt:ident, $py_name:ident, $dtype:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $name {}

        #[typetag::serde]
        impl ScalarUDF for $name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &'static str {
                $name_str
            }

            fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
                match inputs {
                    [input] => match input.to_field(schema) {
                        Ok(field) if field.dtype.is_temporal() => {
                            Ok(Field::new(field.name, DataType::$dtype))
                        }
                        Ok(field) => Err(DaftError::TypeError(format!(
                            "Expected input to {} to be temporal, got {}",
                            self.name(),
                            field.dtype
                        ))),
                        Err(e) => Err(e),
                    },
                    _ => Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    ))),
                }
            }

            fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
                match inputs {
                    [input] => input.$dt(),
                    _ => Err(DaftError::ValueError(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    ))),
                }
            }
        }

        pub fn $dt(input: ExprRef) -> ExprRef {
            ScalarFunction::new($name {}, vec![input]).into()
        }

        // We gotta throw these in a module to avoid name conflicts
        #[cfg(feature = "python")]
        mod $py_name {
            use super::*;
            #[pyfunction]
            pub fn $dt(expr: PyExpr) -> PyResult<PyExpr> {
                Ok(super::$dt(expr.into()).into())
            }
        }
    };
}

impl_temporal!("date", Date, dt_date, py_dt_date, Date);
impl_temporal!("day", Day, dt_day, py_dt_day, UInt32);
impl_temporal!(
    "day_of_week",
    DayOfWeek,
    dt_day_of_week,
    py_dt_day_of_week,
    UInt32
);
impl_temporal!("hour", Hour, dt_hour, py_dt_hour, UInt32);
impl_temporal!("minute", Minute, dt_minute, py_dt_minute, UInt32);
impl_temporal!("month", Month, dt_month, py_dt_month, UInt32);
impl_temporal!("second", Second, dt_second, py_dt_second, UInt32);
impl_temporal!("year", Year, dt_year, py_dt_year, Int32);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Time {}

#[typetag::serde]
impl ScalarUDF for Time {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "time"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => match input.to_field(schema) {
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
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.dt_time(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn dt_time(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Time {}, vec![input]).into()
}

// We gotta throw these in a module to avoid name conflicts
#[cfg(feature = "python")]
mod py_dt_time {
    use super::*;
    #[pyfunction]
    pub fn dt_time(expr: PyExpr) -> PyResult<PyExpr> {
        Ok(super::dt_time(expr.into()).into())
    }
}
