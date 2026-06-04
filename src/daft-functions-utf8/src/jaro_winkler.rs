use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use daft_core::prelude::{Float64Array, IntoSeries};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::jaro::compute_jaro_similarity;

const NULL_SENTINEL: f64 = 0.0;

/// Compute Jaro-Winkler similarity. Applies a prefix bonus to the Jaro similarity
/// for strings that share a common prefix (up to 4 characters).
/// The scaling factor p is fixed at 0.1 (standard value).
fn compute_jaro_winkler_similarity(left: &str, right: &str) -> f64 {
    let jaro = compute_jaro_similarity(left, right);

    // Find common prefix length (max 4 characters)
    let prefix_len = left
        .chars()
        .zip(right.chars())
        .take(4)
        .take_while(|(a, b)| a == b)
        .count();

    let p = 0.1; // Standard Winkler scaling factor

    (prefix_len as f64 * p).mul_add(1.0 - jaro, jaro)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JaroWinklerSimilarity;

#[typetag::serde]
impl ScalarUDF for JaroWinklerSimilarity {
    fn name(&self) -> &'static str {
        "jaro_winkler_similarity"
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
                        (Some(l), Some(r)) => {
                            values.push(compute_jaro_winkler_similarity(l, r));
                            validity.append_non_null();
                        }
                        _ => {
                            values.push(NULL_SENTINEL);
                            validity.append_null();
                        }
                    }
                }

                let field = Arc::new(Field::new(self.name(), DataType::Float64));
                let result = Float64Array::from_field_and_values(field, values)
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
            left.dtype.is_string() || left.dtype == DataType::Null,
            TypeError: "First argument must be a string, got {}",
            left.dtype
        );
        ensure!(
            right.dtype.is_string() || right.dtype == DataType::Null,
            TypeError: "Second argument must be a string, got {}",
            right.dtype
        );

        Ok(Field::new(self.name(), DataType::Float64))
    }

    fn docstring(&self) -> &'static str {
        "Compute the Jaro-Winkler similarity between two strings. This is the Jaro \
        similarity with a prefix bonus for strings sharing a common prefix (up to 4 chars). \
        Returns a value between 0.0 (no similarity) and 1.0 (identical). Returns null when \
        either input is null."
    }
}

#[must_use]
pub fn jaro_winkler_similarity(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(JaroWinklerSimilarity, vec![left, right]).into()
}
