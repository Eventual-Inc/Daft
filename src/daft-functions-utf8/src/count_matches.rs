use std::{iter, sync::Arc};

use aho_corasick::{AhoCorasickBuilder, MatchKind};
use common_error::{DaftError, DaftResult, ensure};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
    lit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CountMatches;

const WHOLE_WORDS_DEFAULT_VALUE: bool = false;
const CASE_SENSITIVE_DEFAULT_VALUE: bool = true;

#[typetag::serde]
impl ScalarUDF for CountMatches {
    fn name(&self) -> &'static str {
        "count_matches"
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

        ensure!(patterns.len() == 1, ValueError: "Cannot set `patterns` in `count_matches` to an Expression. Only string or list of strings are currently supported.");

        input.with_utf8_array(|arr| match patterns.data_type() {
            DataType::Utf8 => patterns.with_utf8_array(|pattern_arr| {
                Ok(
                    count_matches_impl(arr, pattern_arr, whole_words, case_sensitive)?
                        .into_series(),
                )
            }),
            DataType::List(_) => {
                patterns
                    .list()
                    .unwrap()
                    .flat_child
                    .with_utf8_array(|pattern_arr| {
                        Ok(
                            count_matches_impl(arr, pattern_arr, whole_words, case_sensitive)?
                                .into_series(),
                        )
                    })
            }
            patterns_dtype => Err(DaftError::ValueError(format!(
                "expected string or list of strings for 'patterns', got {}",
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
        ensure!(matches!(&patterns.dtype, DataType::List(inner_dtype) if inner_dtype.is_string()) | patterns.dtype.is_string(), ValueError: "expected string or list of strings for 'patterns', got {}", patterns.dtype);

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
        "Count the number of matches for each string in the input array."
    }
}

#[must_use]
pub fn count_matches(
    input: ExprRef,
    patterns: ExprRef,
    whole_words: bool,
    case_sensitive: bool,
) -> ExprRef {
    ScalarFn::builtin(
        CountMatches,
        vec![input, patterns, lit(whole_words), lit(case_sensitive)],
    )
    .into()
}

fn count_matches_impl(
    arr: &Utf8Array,
    patterns: &Utf8Array,
    whole_word: bool,
    case_sensitive: bool,
) -> DaftResult<UInt64Array> {
    if patterns.null_count() == patterns.len() {
        // no matches
        return UInt64Array::from_iter(
            Arc::new(Field::new(arr.name(), DataType::UInt64)),
            iter::repeat_n(Some(0), arr.len()),
        )
        .with_validity(arr.validity().cloned());
    }

    let patterns = patterns.into_iter().flatten();
    let ac = AhoCorasickBuilder::new()
        .ascii_case_insensitive(!case_sensitive)
        .match_kind(MatchKind::LeftmostLongest)
        .build(patterns)
        .map_err(|e| DaftError::ComputeError(format!("Error creating string automaton: {}", e)))?;
    let iter = arr.into_iter().map(|opt| {
        opt.map(|s| {
            let results = ac.find_iter(s);
            if whole_word {
                results
                    .filter(|m| {
                        // ensure this match is a whole word (or set of words)
                        // don't want to filter out things like "brass"
                        let prev_char = s.get(m.start() - 1..m.start());
                        let next_char = s.get(m.end()..=m.end());
                        !(prev_char.is_some_and(|s| s.chars().next().unwrap().is_alphabetic())
                            || next_char.is_some_and(|s| s.chars().next().unwrap().is_alphabetic()))
                    })
                    .count() as u64
            } else {
                results.count() as u64
            }
        })
    });
    Ok(UInt64Array::from_iter(
        Arc::new(Field::new(arr.name(), DataType::UInt64)),
        iter,
    ))
}
