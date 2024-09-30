use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::geo::utils;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GeoDecode {
    pub raise_on_error: bool,
}

impl Default for GeoDecode {
    fn default() -> Self {
        Self {
            raise_on_error: true,
        }
    }
}

#[typetag::serde]
impl ScalarUDF for GeoDecode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "geo_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::Binary | DataType::Utf8 => {
                        Ok(Field::new(field.name, DataType::Geometry))
                    }
                    // DataType::List(Box<DataType::UInt8>) => Ok(Field::new(field.name, DataType::Geometry)),
                    _ => Err(DaftError::TypeError(format!(
                        "GeoDecode can only decode Binary or Utf8 arrays, got {}",
                        field
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let raise_error_on_failure = self.raise_on_error;
        match inputs {
            [input] => match input.data_type() {
                DataType::Binary | DataType::Utf8 => {
                    utils::decode_series(input, raise_error_on_failure)
                }
                other => Err(DaftError::TypeError(format!(
                    "GeoDecode can only decode Binary or Utf8 arrays, got {}",
                    other
                ))),
            },
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn decode(input: ExprRef, args: Option<GeoDecode>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "geo_decode")]
pub fn py_decode(expr: PyExpr, raise_on_error: Option<bool>) -> PyResult<PyExpr> {
    let geo_decode = GeoDecode {
        raise_on_error: raise_on_error.unwrap_or(true),
    };

    Ok(decode(expr.into(), Some(geo_decode)).into())
}
