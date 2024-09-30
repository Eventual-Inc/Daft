use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::geo::utils;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GeoEncode {
    pub text: bool,
    pub raise_on_error: bool,
}

impl Default for GeoEncode {
    fn default() -> Self {
        Self {
            text: false,
            raise_on_error: true,
        }
    }
}

#[typetag::serde]
impl ScalarUDF for GeoEncode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "geo_encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::Geometry => match self.text {
                        true => Ok(Field::new(field.name, DataType::Utf8)),
                        false => Ok(Field::new(field.name, DataType::Binary)),
                    },
                    _ => Err(DaftError::TypeError(format!(
                        "GeoEncode can only encode Geometry arrays, got {}",
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
        match inputs {
            [input] => match input.data_type() {
                DataType::Geometry => utils::encode_series(input, self.text),
                other => Err(DaftError::TypeError(format!(
                    "GeoDecode can only decode Geometry arrays, got {}",
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

pub fn encode(input: ExprRef, args: Option<GeoEncode>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "geo_encode")]
pub fn py_encode(
    expr: PyExpr,
    text: Option<bool>,
    raise_on_error: Option<bool>,
) -> PyResult<PyExpr> {
    let geo_decode = GeoEncode {
        text: text.unwrap_or(false),
        raise_on_error: raise_on_error.unwrap_or(true),
    };

    Ok(encode(expr.into(), Some(geo_decode)).into())
}
