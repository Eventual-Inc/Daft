use common_error::{DaftError, DaftResult};
use common_hashable_float_wrapper::FloatWrapper;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

// super annoying, but using an enum with typetag::serde doesn't work with bincode because it uses Deserializer::deserialize_identifier
macro_rules! log {
    ($name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &'static str {
                stringify!($name)
            }

            fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
                if inputs.len() != 1 {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    )));
                };
                let field = inputs.first().unwrap().to_field(schema)?;
                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to log to be numeric, got {}",
                            field.dtype
                        )))
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
                evaluate_single_numeric(inputs, Series::$name)
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFunction::new($variant, vec![input]).into()
        }
    };
}

log!(log2, Log2);
log!(log10, Log10);
log!(ln, Ln);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Log(FloatWrapper<f64>);

#[typetag::serde]
impl ScalarUDF for Log {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "log"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 1 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        }
        let field = inputs.first().unwrap().to_field(schema)?;
        let dtype = match field.dtype {
            DataType::Float32 => DataType::Float32,
            dt if dt.is_numeric() => DataType::Float64,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected input to log to be numeric, got {}",
                    field.dtype
                )))
            }
        };
        Ok(Field::new(field.name, dtype))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        evaluate_single_numeric(inputs, |x| x.log(self.0 .0))
    }
}

#[must_use]
pub fn log(input: ExprRef, base: f64) -> ExprRef {
    ScalarFunction::new(Log(FloatWrapper(base)), vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

use super::evaluate_single_numeric;

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "log2")]
pub fn py_log2(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(log2(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "log10")]
pub fn py_log10(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(log10(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "log")]
pub fn py_log(expr: PyExpr, base: f64) -> PyResult<PyExpr> {
    Ok(log(expr.into(), base).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "ln")]
pub fn py_ln(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(ln(expr.into()).into())
}
