use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use daft_core::prelude::{Int64Array, IntoSeries};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

const NULL_SENTINEL: i64 = 0;

fn hamming_distance_str(left: &str, right: &str) -> Option<i64> {
    let mut left_chars = left.chars();
    let mut right_chars = right.chars();

    let mut distance = 0i64;

    loop {
        match (left_chars.next(), right_chars.next()) {
            (Some(l), Some(r)) => {
                if l != r {
                    distance += 1;
                }
            }
            (None, None) => return Some(distance),
            _ => return None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HammingDistance;

#[typetag::serde]
impl ScalarUDF for HammingDistance {
    fn name(&self) -> &'static str {
        "hamming_distance"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let left = inputs.required(0)?.cast(&DataType::Utf8)?;
        let right = inputs.required(1)?.cast(&DataType::Utf8)?;

        left.with_utf8_array(|left| {
            right.with_utf8_array(|right| {
                let len = left.len();
                let mut values = Vec::with_capacity(len);
                let mut validity = NullBufferBuilder::new(len);

                for i in 0..len {
                    match (left.get(i), right.get(i)) {
                        (Some(l), Some(r)) => match hamming_distance_str(l, r) {
                            Some(dist) => {
                                values.push(dist);
                                validity.append_non_null();
                            }
                            None => {
                                values.push(NULL_SENTINEL);
                                validity.append_null();
                            }
                        },
                        _ => {
                            values.push(NULL_SENTINEL);
                            validity.append_null();
                        }
                    }
                }

                let field = Arc::new(Field::new(self.name(), DataType::Int64));
                let result = Int64Array::from_field_and_values(field, values)
                    .with_nulls(validity.finish())?;
                Ok(result.into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 inputs, but received {}",
            inputs.len()
        );

        let left = inputs.required(0)?.to_field(schema)?;
        let right = inputs.required(1)?.to_field(schema)?;

        ensure!(
            left.dtype.is_string(),
            TypeError: "First argument must be a string, got {}",
            left.dtype
        );
        ensure!(
            right.dtype.is_string(),
            TypeError: "Second argument must be a string, got {}",
            right.dtype
        );

        Ok(Field::new(self.name(), DataType::Int64))
    }

    fn docstring(&self) -> &'static str {
        "Calculate the Hamming distance between two UTF-8 strings."
    }
}

#[must_use]
pub fn hamming_distance(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(HammingDistance, vec![left, right]).into()
}
