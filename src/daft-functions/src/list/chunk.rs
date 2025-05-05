use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListChunk {
    pub size: usize,
}

#[typetag::serde]
impl ScalarUDF for ListChunk {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "chunk"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(input_field
                    .to_exploded_field()?
                    .to_fixed_size_list_field(self.size)?
                    .to_list_field()?)
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.list_chunk(self.size),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_chunk(expr: ExprRef, size: usize) -> ExprRef {
    ScalarFunction::new(ListChunk { size }, vec![expr]).into()
}
