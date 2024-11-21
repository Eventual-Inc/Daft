use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageToMode {
    pub mode: ImageMode,
}

#[typetag::serde]
impl ScalarUDF for ImageToMode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_mode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                let output_dtype = match field.dtype {
                    DataType::Image(_) => DataType::Image(Some(self.mode)),
                    DataType::FixedShapeImage(_, h, w) => {
                        DataType::FixedShapeImage(self.mode, h, w)
                    }
                    _ => {
                        return Err(DaftError::TypeError(format!(
                        "ToMode can only operate on ImageArrays and FixedShapeImageArrays, got {field}"
                    )))
                    }
                };
                Ok(Field::new(field.name, output_dtype))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => daft_image::series::to_mode(input, self.mode),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn image_to_mode(expr: ExprRef, mode: ImageMode) -> ExprRef {
    ScalarFunction::new(ImageToMode { mode }, vec![expr]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "image_to_mode")]
pub fn py_image_to_mode(expr: PyExpr, mode: ImageMode) -> PyResult<PyExpr> {
    Ok(image_to_mode(expr.into(), mode).into())
}
