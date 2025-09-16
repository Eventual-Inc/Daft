use std::sync::Arc;

use common_error::{DaftError, DaftResult, ensure};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
    lit,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpCountMatches;

const WHOLE_WORDS_DEFAULT_VALUE: bool = false;
const CASE_SENSITIVE_DEFAULT_VALUE: bool = true;

#[typetag::serde]
impl ScalarUDF for RegexpCountMatches {
    fn name(&self) -> &'static str {
        "count_matches_regex"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let patterns = inputs.required((1, "patterns"))?;

        let whole_words = inputs.optional("whole_words")?.map(|s| {
            ensure!(s.data_type().is_boolean() && s.len() == 1, ValueError: "expected boolean literal for 'whole_words'");
            Ok(s.bool().unwrap().get(0).unwrap())
        }).transpose()?.unwrap_or(WHOLE_WORDS_DEFAULT_VALUE);
        let case_sensitive = inputs.optional("case_sensitive")?.map(|s| {
            ensure!(s.data_type().is_boolean() && s.len() == 1, ValueError: "expected boolean literal for 'case_sensitive'");
            Ok(s.bool().unwrap().get(0).unwrap())
        }).transpose()?.unwrap_or(CASE_SENSITIVE_DEFAULT_VALUE);

        ensure!(patterns.len() == 1, ValueError: "Cannot set `patterns` in `count_matches_regex` to an Expression. Only string is currently supported.");

        input.with_utf8_array(|arr| match patterns.data_type() {
            DataType::Utf8 => patterns.with_utf8_array(|pattern_arr| {
                Ok(
                    regex_count_matches_impl(arr, pattern_arr, whole_words, case_sensitive)?
                        .into_series(),
                )
            }),
            patterns_dtype => Err(DaftError::ValueError(format!(
                "expected string for 'patterns', got {}",
                patterns_dtype
            ))),
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let patterns = inputs.required((1, "patterns"))?.to_field(schema)?;
        ensure!(patterns.dtype.is_string(), ValueError: "expected string for 'patterns', got {}", patterns.dtype);

        if let Some(whole_words) = inputs.optional("whole_words")? {
            whole_words
                .as_literal()
                .and_then(|lit| lit.as_bool())
                .ok_or(DaftError::ValueError(
                    "expected boolean literal for 'whole_words'".to_string(),
                ))?;
        }

        if let Some(case_sensitive) = inputs.optional("case_sensitive")? {
            case_sensitive
                .as_literal()
                .and_then(|lit| lit.as_bool())
                .ok_or(DaftError::ValueError(
                    "expected boolean literal for 'case_sensitive'".to_string(),
                ))?;
        }

        Ok(Field::new(input.name, DataType::UInt64))
    }

    fn docstring(&self) -> &'static str {
        "Count the number of regex matches for each string in the input array."
    }
}

#[must_use]
pub fn regexp_count_matches(
    input: ExprRef,
    patterns: ExprRef,
    whole_words: bool,
    case_sensitive: bool,
) -> ExprRef {
    ScalarFn::builtin(
        RegexpCountMatches,
        vec![input, patterns, lit(whole_words), lit(case_sensitive)],
    )
    .into()
}

fn regex_count_matches_impl(
    arr: &Utf8Array,
    patterns: &Utf8Array,
    whole_words: bool,
    case_sensitive: bool,
) -> DaftResult<UInt64Array> {
    let pattern_str = patterns
        .get(0)
        .ok_or_else(|| DaftError::ValueError("Pattern cannot be null".to_string()))?;

    // For regex, we ignore whole_words and case_sensitive parameters
    // as regex patterns handle these directly
    let _ = (whole_words, case_sensitive);

    let regex = regex::Regex::new(pattern_str)
        .map_err(|e| DaftError::ValueError(format!("Invalid regex pattern: {}", e)))?;

    let iter = arr
        .as_arrow()
        .iter()
        .map(|opt| opt.map(|s| regex.find_iter(s).count() as u64));

    Ok(UInt64Array::from_iter(
        Arc::new(Field::new(arr.name(), DataType::UInt64)),
        iter,
    ))
}
