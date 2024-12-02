use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::InferDataType,
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Clip;

#[typetag::serde]
impl ScalarUDF for Clip {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "clip"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 3 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
        let array_field = inputs[0].to_field(schema)?;
        let min_field = inputs[1].to_field(schema)?;
        let max_field = inputs[2].to_field(schema)?;

        let output_type = InferDataType::clip_op(
            &InferDataType::from(&array_field.dtype),
            &InferDataType::from(&min_field.dtype),
            &InferDataType::from(&max_field.dtype),
        )?;

        Ok(Field::new(array_field.name, output_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
        let array = &inputs[0];
        let min = &inputs[1];
        let max = &inputs[2];

        array.clip(min, max)
    }
}

#[must_use]
pub fn clip(array: ExprRef, min: ExprRef, max: ExprRef) -> ExprRef {
    ScalarFunction::new(Clip, vec![array, min, max]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "clip")]
pub fn py_clip(array: PyExpr, min: PyExpr, max: PyExpr) -> PyResult<PyExpr> {
    Ok(clip(array.into(), min.into(), max.into()).into())
}
