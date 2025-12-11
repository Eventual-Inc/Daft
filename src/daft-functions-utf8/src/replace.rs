#![allow(deprecated, reason = "arrow2 migration")]
use std::{borrow::Borrow, sync::LazyLock};

use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    prelude::{AsArrow, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpReplace;

#[typetag::serde]
impl ScalarUDF for RegexpReplace {
    fn name(&self) -> &'static str {
        "regexp_replace"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, true)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_return_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string using a regular expression"
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Replace;

#[typetag::serde]
impl ScalarUDF for Replace {
    fn name(&self) -> &'static str {
        "replace"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, false)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_return_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string"
    }
}

#[must_use]
pub fn replace(input: ExprRef, pattern: ExprRef, replacement: ExprRef, regex: bool) -> ExprRef {
    let inputs = vec![input, pattern, replacement];

    if regex {
        ScalarFn::builtin(RegexpReplace, inputs).into()
    } else {
        ScalarFn::builtin(Replace, inputs).into()
    }
}

fn get_return_field_impl(inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
    ensure!(inputs.len() == 3, "Replace expects 3 arguments");
    let input = inputs.required((0, "input"))?.to_field(schema)?;
    let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
    let replacement = inputs.required((2, "replacement"))?.to_field(schema)?;
    ensure!(
        input.dtype.is_string(), TypeError: "Input must be of type Utf8"
    );
    ensure!(
        pattern.dtype.is_string(), TypeError: "Pattern must be of type Utf8"
    );
    ensure!(
        replacement.dtype.is_string(), TypeError: "Replacement must be of type Utf8"
    );

    Ok(input)
}

fn series_replace(
    s: &Series,
    pattern: &Series,
    replacement: &Series,
    regex: bool,
) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern_arr| {
            replacement.with_utf8_array(|replacement_arr| {
                Ok(replace_impl(arr, pattern_arr, replacement_arr, regex)?.into_series())
            })
        })
    })
}

fn replace_impl(
    arr: &Utf8Array,
    pattern: &Utf8Array,
    replacement: &Utf8Array,
    regex: bool,
) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern, replacement])
        .map_err(|e| DaftError::ValueError(format!("Error in replace: {e}")))?;
    if is_full_null {
        return Ok(Utf8Array::full_null(
            arr.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Utf8Array::empty(arr.name(), &DataType::Utf8));
    }

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    let replacement_iter = create_broadcasted_str_iter(replacement, expected_size);

    let result = match (regex, pattern.len()) {
        (true, 1) => {
            let regex_val = regex::Regex::new(pattern.get(0).unwrap());
            let regex = regex_val.as_ref().map_err(|e| e.clone());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            regex_replace(arr_iter, regex_iter, replacement_iter, arr.name())?
        }
        (true, _) => {
            let regex_iter = pattern
                .as_arrow2()
                .iter()
                .map(|pat| pat.map(regex::Regex::new));
            regex_replace(arr_iter, regex_iter, replacement_iter, arr.name())?
        }
        (false, _) => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            replace_on_literal(arr_iter, pattern_iter, replacement_iter, arr.name())?
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

/// replace POSIX capture groups (like \1) with Rust Regex group (like ${1})
/// used by regexp_replace
fn regex_replace_posix_groups(replacement: &str) -> String {
    static CAPTURE_GROUPS_RE_LOCK: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(\\)(\d*)").unwrap());
    CAPTURE_GROUPS_RE_LOCK
        .replace_all(replacement, "$${$2}")
        .into_owned()
}

fn regex_replace<'a, R: Borrow<regex::Regex>>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<R, regex::Error>>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(regex_iter)
        .zip(replacement_iter)
        .map(|((val, re), replacement)| match (val, re, replacement) {
            (Some(val), Some(re), Some(replacement)) => {
                let replacement = regex_replace_posix_groups(replacement);
                Ok(Some(re?.borrow().replace_all(val, replacement.as_str())))
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<daft_arrow::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}
fn replace_on_literal<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    pattern_iter: impl Iterator<Item = Option<&'a str>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(pattern_iter)
        .zip(replacement_iter)
        .map(|((val, pat), replacement)| match (val, pat, replacement) {
            (Some(val), Some(pat), Some(replacement)) => Ok(Some(val.replace(pat, replacement))),
            _ => Ok(None),
        })
        .collect::<DaftResult<daft_arrow::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}
