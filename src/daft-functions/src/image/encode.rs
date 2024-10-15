use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

/// Container for the keyword arguments for `image_encode`
/// ex:
/// ```text
/// image_encode(input, image_format='png')
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageEncode {
    pub image_format: ImageFormat,
}

#[typetag::serde]
impl ScalarUDF for ImageEncode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "image_encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::Image(..) | DataType::FixedShapeImage(..) => {
                        Ok(Field::new(field.name, DataType::Binary))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "ImageEncode can only encode ImageArrays and FixedShapeImageArrays, got {field}"
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
            [input] => daft_image::series::encode(input, self.image_format),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn encode(input: ExprRef, image_encode: ImageEncode) -> ExprRef {
    ScalarFunction::new(image_encode, vec![input]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "image_encode")]
pub fn py_encode(expr: PyExpr, image_format: ImageFormat) -> PyResult<PyExpr> {
    let image_encode = ImageEncode { image_format };
    Ok(encode(expr.into(), image_encode).into())
}
