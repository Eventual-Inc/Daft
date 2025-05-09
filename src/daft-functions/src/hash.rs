use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct HashFunction;

#[typetag::serde]
impl ScalarUDF for HashFunction {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn name(&self) -> &'static str {
        "hash"
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => input.hash(None).map(|arr| arr.into_series()),
            [input, seed] => {
                match seed.len() {
                    1 => {
                        let seed = seed.cast(&DataType::UInt64)?;
                        // There's no way to natively extend the array, so we extract the element and repeat it.
                        let seed = seed.u64().unwrap();
                        let seed = seed.get(0).unwrap();
                        let seed = UInt64Array::from_iter(
                            Field::new("seed", DataType::UInt64),
                            std::iter::repeat_n(Some(seed), input.len()),
                        );
                        input
                            .hash(Some(&seed))
                            .map(daft_core::series::IntoSeries::into_series)
                    }
                    _ if seed.len() == input.len() => {
                        let seed = seed.cast(&DataType::UInt64)?;
                        let seed = seed.u64().unwrap();

                        input
                            .hash(Some(seed))
                            .map(daft_core::series::IntoSeries::into_series)
                    }
                    _ => Err(DaftError::ValueError(
                        "Seed must be a single value or the same length as the input".to_string(),
                    )),
                }
            }
            _ => Err(DaftError::ValueError("Expected 2 input arg".to_string())),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] | [input, _] => match input.to_field(schema) {
                Ok(field) => Ok(Field::new(field.name, DataType::UInt64)),
                e => e,
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn hash(input: ExprRef, seed: Option<ExprRef>) -> ExprRef {
    let inputs = match seed {
        Some(seed) => vec![input, seed],
        None => vec![input],
    };

    ScalarFunction::new(HashFunction {}, inputs).into()
}
