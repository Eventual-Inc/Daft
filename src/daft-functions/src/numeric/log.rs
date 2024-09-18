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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Log {
    Log2,
    Log10,
    Log(FloatWrapper<f64>),
    Ln,
}

#[typetag::serde]
impl ScalarUDF for Log {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        match self {
            Log::Log2 => "log2",
            Log::Log10 => "log10",
            Log::Log(_) => "log",
            Log::Ln => "ln",
        }
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
        match inputs {
            [input] => match self {
                Log::Log2 => input.log2(),
                Log::Log10 => input.log10(),
                Log::Log(base) => input.log(base.0),
                Log::Ln => input.ln(),
            },
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn log2(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Log::Log2, vec![input]).into()
}

pub fn log10(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Log::Log10, vec![input]).into()
}

pub fn log(input: ExprRef, base: f64) -> ExprRef {
    ScalarFunction::new(Log::Log(FloatWrapper(base)), vec![input]).into()
}

pub fn ln(input: ExprRef) -> ExprRef {
    ScalarFunction::new(Log::Ln, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

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
