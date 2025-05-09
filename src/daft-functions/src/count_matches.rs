use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CountMatchesFunction {
    pub whole_words: bool,
    pub case_sensitive: bool,
}

#[typetag::serde]
impl ScalarUDF for CountMatchesFunction {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "count_matches"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, _] => match data.to_field(schema) {
                Ok(field) => match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(field.name, DataType::UInt64)),
                    a => Err(DaftError::TypeError(format!(
                        "Expects inputs to count_matches to be utf8, but received {a}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, patterns] => {
                data.utf8_count_matches(patterns, self.whole_words, self.case_sensitive)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn utf8_count_matches(
    input: ExprRef,
    patterns: ExprRef,
    whole_words: bool,
    case_sensitive: bool,
) -> ExprRef {
    ScalarFunction::new(
        CountMatchesFunction {
            whole_words,
            case_sensitive,
        },
        vec![input, patterns],
    )
    .into()
}
