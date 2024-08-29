use daft_core::{
    datatypes::{DataType, Field, ImageMode},
    schema::Schema,
    series::Series,
};

use common_error::{DaftError, DaftResult};
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
pub struct ImageDecodeArgs {
    pub mode: Option<ImageMode>,
    pub raise_on_error: bool,
}

impl Default for ImageDecodeArgs {
    fn default() -> Self {
        Self {
            mode: None,
            raise_on_error: true,
        }
    }
}

#[typetag::serde]
impl ScalarUDF for ImageDecodeArgs {
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
                        "ImageDecode can only decode BinaryArrays, got {}",
                        field
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
            [input] => input.image_decode(raise_error_on_failure, self.mode),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn decode(input: ExprRef, args: Option<ImageDecodeArgs>) -> ExprRef {
    ScalarFunction::new(args.unwrap_or_default(), vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, types::PyDict, PyResult},
};

#[cfg(feature = "python")]
impl TryFrom<Option<&PyDict>> for ImageDecodeArgs {
    type Error = pyo3::PyErr;

    fn try_from(py_kwargs: Option<&PyDict>) -> Result<Self, Self::Error> {
        let mut mode = None;
        let mut raise_on_error = false;
        if let Some(kwargs) = py_kwargs {
            if let Some(mode_str) = kwargs.get_item("mode") {
                mode = mode_str.extract()?;
            }
            if let Some(raise_on_error_str) = kwargs.get_item("raise_on_error") {
                raise_on_error = raise_on_error_str.extract()?;
            }
        }
        Ok(Self {
            mode,
            raise_on_error,
        })
    }
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "image_decode", signature = (expr, **kwds))]
pub fn py_decode(expr: PyExpr, kwds: Option<&PyDict>) -> PyResult<PyExpr> {
    let kwargs = ImageDecodeArgs::try_from(kwds)?;
    Ok(decode(expr.into(), Some(kwargs)).into())
}
