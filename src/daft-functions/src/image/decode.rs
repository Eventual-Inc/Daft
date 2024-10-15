use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

/// Container for the keyword arguments for `image_decode`
/// ex:
/// ```text
/// image_decode(input)
/// image_decode(input, mode='RGB')
/// image_decode(input, mode='RGB', on_error='raise')
/// image_decode(input, on_error='null')
/// image_decode(input, on_error='null', mode='RGB')
/// image_decode(input, mode='RGB', on_error='null')
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageDecode {
    pub mode: Option<ImageMode>,
    pub raise_on_error: bool,
}

impl Default for ImageDecode {
    fn default() -> Self {
        Self {
            mode: None,
            raise_on_error: true,
        }
    }
}

#[typetag::serde]
impl ScalarUDF for ImageDecode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "image_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                if !matches!(field.dtype, DataType::Binary) {
                    return Err(DaftError::TypeError(format!(
                        "ImageDecode can only decode BinaryArrays, got {field}"
                    )));
                }
                Ok(Field::new(field.name, DataType::Image(self.mode)))
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
            [input] => daft_image::series::decode(input, raise_error_on_failure, self.mode),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn decode(input: ExprRef, args: Option<ImageDecode>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "image_decode")]
pub fn py_decode(
    expr: PyExpr,
    raise_on_error: Option<bool>,
    mode: Option<ImageMode>,
) -> PyResult<PyExpr> {
    let image_decode = ImageDecode {
        mode,
        raise_on_error: raise_on_error.unwrap_or(true),
    };

    Ok(decode(expr.into(), Some(image_decode)).into())
}
