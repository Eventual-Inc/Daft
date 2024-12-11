use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct ListContains;

#[typetag::serde]
impl ScalarUDF for ListContains {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_contains"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let [input, contains] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        };
        let input = input.to_field(schema)?;
        let (DataType::List(internal_dt) | DataType::FixedSizeList(internal_dt, _)) = input.dtype
        else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a list type, received: {}",
                input.dtype,
            )));
        };
        let contains = contains.to_field(schema)?;
        if internal_dt.as_ref() != &contains.dtype {
            return Err(DaftError::TypeError(format!(
                "Can't match input, which is a list of type {}, against contains, which is of type {}",
                internal_dt, contains.dtype,
            )));
        };

        Ok(Field::new(input.name, DataType::Boolean))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let [input, contains] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        };
        input.list_contains(contains)
    }
}

#[must_use]
pub fn list_contains(expr: ExprRef, contains: ExprRef) -> ExprRef {
    ScalarFunction::new(ListContains, vec![expr, contains]).into()
}
