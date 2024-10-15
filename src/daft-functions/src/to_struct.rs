use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

fn series_to_struct(inputs: &[Series]) -> Series {
    let child_fields: Vec<Field> = inputs.iter().map(|s| s.field().clone()).collect();
    let field = Field::new("struct", DataType::Struct(child_fields));
    let inputs = inputs.to_vec();
    StructArray::new(field, inputs, None).into_series()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct ToStructFunction {}

#[typetag::serde]
impl ScalarUDF for ToStructFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_struct"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call to_struct with no inputs".to_string(),
            ));
        }
        Ok(series_to_struct(inputs))
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call to_struct with no inputs".to_string(),
            ));
        }
        let child_fields = inputs
            .iter()
            .map(|e| e.to_field(schema))
            .collect::<DaftResult<_>>()?;
        Ok(Field::new("struct", DataType::Struct(child_fields)))
    }
}

#[must_use]
pub fn to_struct(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFunction::new(ToStructFunction {}, inputs).into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn to_struct(inputs: Vec<PyExpr>) -> PyResult<PyExpr> {
        let inputs = inputs.into_iter().map(std::convert::Into::into).collect();
        let expr = super::to_struct(inputs);
        Ok(expr.into())
    }
}
