use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_str_distance, binary_str_distance_to_field};

/// Compute the Damerau-Levenshtein distance (optimal string alignment variant).
/// This extends Levenshtein by also allowing transposition of two adjacent characters
/// as a single edit operation.
fn compute_damerau_levenshtein_distance(left: &str, right: &str) -> i64 {
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

    // Full matrix needed for transposition lookback
    let mut matrix = vec![vec![0i64; m + 1]; n + 1];

    for (i, row) in matrix.iter_mut().enumerate() {
        row[0] = i as i64;
    }
    for j in 0..=m {
        matrix[0][j] = j as i64;
    }

    for i in 1..=n {
        for j in 1..=m {
            let cost = i64::from(left_chars[i - 1] != right_chars[j - 1]);

            matrix[i][j] = (matrix[i - 1][j] + 1) // deletion
                .min(matrix[i][j - 1] + 1) // insertion
                .min(matrix[i - 1][j - 1] + cost); // substitution

            // Transposition
            if i > 1
                && j > 1
                && left_chars[i - 1] == right_chars[j - 2]
                && left_chars[i - 2] == right_chars[j - 1]
            {
                matrix[i][j] = matrix[i][j].min(matrix[i - 2][j - 2] + 1);
            }
        }
    }

    matrix[n][m]
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DamerauLevenshteinDistance;

#[typetag::serde]
impl ScalarUDF for DamerauLevenshteinDistance {
    fn name(&self) -> &'static str {
        "damerau_levenshtein_distance"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_str_distance::<daft_core::datatypes::Int64Type, _>(
            inputs,
            self.name(),
            DataType::Int64,
            compute_damerau_levenshtein_distance,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_str_distance_to_field(inputs, schema, self.name(), DataType::Int64)
    }

    fn docstring(&self) -> &'static str {
        "Compute the Damerau-Levenshtein distance between two strings. This extends the \
        Levenshtein distance by also counting transpositions of two adjacent characters \
        as a single edit operation. This computes the Optimal String Alignment (OSA) \
        variant, which may differ from true Damerau-Levenshtein for inputs with \
        overlapping transpositions. Returns null when either input is null."
    }
}

#[must_use]
pub fn damerau_levenshtein_distance(left: ExprRef, right: ExprRef) -> ExprRef {
    ScalarFn::builtin(DamerauLevenshteinDistance, vec![left, right]).into()
}
