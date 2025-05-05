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
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inner = inputs.into_inner();
        self.evaluate_from_series(&inner)
    }

    fn name(&self) -> &'static str {
        "struct"
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call struct with no inputs".to_string(),
            ));
        }
        Ok(series_to_struct(inputs))
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call struct with no inputs".to_string(),
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
