use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use daft_core::prelude::{Float64Array, IntoSeries};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

const NULL_SENTINEL: f64 = 0.0;

/// Compute Jaro similarity between two strings.
/// Returns a value between 0.0 (no similarity) and 1.0 (identical).
pub(crate) fn compute_jaro_similarity(left: &str, right: &str) -> f64 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();

    let s1_len = left_chars.len();
    let s2_len = right_chars.len();

    if s1_len == 0 && s2_len == 0 {
        return 1.0;
    }
    if s1_len == 0 || s2_len == 0 {
        return 0.0;
    }

    // Maximum distance for matching characters
    let match_distance = (s1_len.max(s2_len) / 2).saturating_sub(1);

    let mut s1_matches = vec![false; s1_len];
    let mut s2_matches = vec![false; s2_len];

    let mut matches: f64 = 0.0;
    let mut transpositions: f64 = 0.0;

    // Find matching characters
    for i in 0..s1_len {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(s2_len);

        for j in start..end {
            if s2_matches[j] || left_chars[i] != right_chars[j] {
                continue;
            }
            s1_matches[i] = true;
            s2_matches[j] = true;
            matches += 1.0;
            break;
        }
    }

    if matches == 0.0 {
        return 0.0;
    }

    // Count transpositions
    let mut k = 0;
    for i in 0..s1_len {
        if !s1_matches[i] {
            continue;
        }
        while !s2_matches[k] {
            k += 1;
        }
        if left_chars[i] != right_chars[k] {
            transpositions += 1.0;
        }
        k += 1;
    }

    (matches / s1_len as f64 + matches / s2_len as f64 + (matches - transpositions / 2.0) / matches)
        / 3.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JaroSimilarity;

#[typetag::serde]
impl ScalarUDF for JaroSimilarity {
    fn name(&self) -> &'static str {
        "jaro_similarity"
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
                            values.push(compute_jaro_similarity(l, r));
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
        "Compute the Jaro similarity between two strings. Returns a value between 0.0 \
        (no similarity) and 1.0 (identical strings). Returns null when either input is null."
    }
}

#[must_use]
pub fn jaro_similarity(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(JaroSimilarity, vec![left, right]).into()
}
