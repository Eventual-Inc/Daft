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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageDecodeFunction {
    mode: Option<ImageMode>,
    raise_error_on_failure: bool,
}

#[typetag::serde]
impl ScalarUDF for ImageDecodeFunction {
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
        let raise_error_on_failure = self.raise_error_on_failure;
        match inputs {
            [input] => input.image_decode(raise_error_on_failure, self.mode),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn decode(input: ExprRef, raise_error_on_failure: bool, mode: Option<ImageMode>) -> ExprRef {
    ScalarFunction::new(
        ImageDecodeFunction {
            mode,
            raise_error_on_failure,
        },
        vec![input],
    )
    .into()
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
    input: PyExpr,
    raise_error_on_failure: bool,
    mode: Option<ImageMode>,
) -> PyResult<PyExpr> {
    Ok(decode(input.into(), raise_error_on_failure, mode).into())
}
