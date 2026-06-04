use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use daft_core::prelude::{Int64Array, IntoSeries};
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

const NULL_SENTINEL: i64 = 0;

/// Compute Levenshtein edit distance using Wagner-Fischer algorithm.
/// Uses O(min(n,m)) space by only keeping two rows of the DP matrix.
fn compute_levenshtein_distance(left: &str, right: &str) -> i64 {
    let left_chars: Vec<char> = left.chars().collect();
    let right_chars: Vec<char> = right.chars().collect();

    let n = left_chars.len();
    let m = right_chars.len();

    if n == 0 {
        return m as i64;
    }
    if m == 0 {
        return n as i64;
    }

    // Ensure we iterate over the shorter string for the inner loop
    let (shorter, longer, short_len, long_len) = if n <= m {
        (&left_chars, &right_chars, n, m)
    } else {
        (&right_chars, &left_chars, m, n)
    };

    let mut prev_row: Vec<i64> = (0..=(short_len as i64)).collect();
    let mut curr_row: Vec<i64> = vec![0; short_len + 1];

    for i in 1..=long_len {
        curr_row[0] = i as i64;
        for j in 1..=short_len {
            let cost = i64::from(longer[i - 1] != shorter[j - 1]);
            curr_row[j] = (prev_row[j] + 1) // deletion
                .min(curr_row[j - 1] + 1) // insertion
                .min(prev_row[j - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[short_len]
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LevenshteinDistance;

#[typetag::serde]
impl ScalarUDF for LevenshteinDistance {
    fn name(&self) -> &'static str {
        "levenshtein_distance"
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
                            values.push(compute_levenshtein_distance(l, r));
                            validity.append_non_null();
                        }
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
            left.dtype.is_string() || left.dtype == DataType::Null,
            TypeError: "First argument must be a string, got {}",
            left.dtype
        );
        ensure!(
            right.dtype.is_string() || right.dtype == DataType::Null,
            TypeError: "Second argument must be a string, got {}",
            right.dtype
        );

        Ok(Field::new(self.name(), DataType::Int64))
    }

    fn docstring(&self) -> &'static str {
        "Compute the Levenshtein edit distance between two strings. The Levenshtein distance \
        is the minimum number of single-character insertions, deletions, or substitutions \
        required to transform one string into the other. Returns null when either input is null."
    }
}

#[must_use]
pub fn levenshtein_distance(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(LevenshteinDistance, vec![left, right]).into()
}
