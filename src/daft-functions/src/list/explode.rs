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
pub struct Explode {}

#[typetag::serde]
impl ScalarUDF for Explode {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "explode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                field.to_exploded_field()
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.explode(),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn explode(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(Explode {}, vec![expr]).into()
}
