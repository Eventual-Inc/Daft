use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

use crate::{
    jaro::compute_jaro_similarity,
    utils::{binary_str_distance, binary_str_distance_to_field},
};

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
        binary_str_distance::<daft_core::datatypes::Float64Type, _>(
            inputs,
            self.name(),
            DataType::Float64,
            compute_jaro_winkler_similarity,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_str_distance_to_field(inputs, schema, self.name(), DataType::Float64)
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
