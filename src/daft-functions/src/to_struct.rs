use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, ScalarFunction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct ToStructFunction;

#[typetag::serde]
impl ScalarUDF for ToStructFunction {
    fn name(&self) -> &'static str {
        "struct"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call struct with no inputs".to_string(),
            ));
        }
        let child_fields: Vec<Field> = inputs.iter().map(|s| s.field().clone()).collect();
        let field = Field::new("struct", DataType::Struct(child_fields));

        Ok(StructArray::new(field, inputs, None).into_series())
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
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
    ScalarFunction::new(ToStructFunction, inputs).into()
}
