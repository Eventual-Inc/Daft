use std::sync::Arc;

use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArg, FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpReplace;

#[typetag::serde]
impl ScalarUDF for RegexpReplace {
    fn name(&self) -> &'static str {
        "regexp_replace"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, true)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        function_args_to_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string using a regular expression"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Replace;

#[typetag::serde]
impl ScalarUDF for Replace {
    fn name(&self) -> &'static str {
        "replace"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let replacement = inputs.required((2, "replacement"))?;
        series_replace(input, pattern, replacement, false)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        function_args_to_field_impl(inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Replaces all occurrences of a substring with a new string"
    }
}

#[must_use]
pub fn replace(input: ExprRef, pattern: ExprRef, replacement: ExprRef, regex: bool) -> ExprRef {
    ScalarFunction {
        udf: if regex {
            Arc::new(RegexpReplace) as _
        } else {
            Arc::new(Replace) as _
        },
        inputs: FunctionArgs::new_unchecked(vec![
            FunctionArg::unnamed(input),
            FunctionArg::unnamed(pattern),
            FunctionArg::unnamed(replacement),
        ]),
    }
    .into()
}

fn function_args_to_field_impl(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
) -> DaftResult<Field> {
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
            let regex = regex::Regex::new(pattern.get(0).unwrap());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            regex_replace(arr_iter, regex_iter, replacement_iter, arr.name())?
        }
        (true, _) => {
            let regex_iter = pattern
                .as_arrow()
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

fn regex_replace<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    replacement_iter: impl Iterator<Item = Option<&'a str>>,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(regex_iter)
        .zip(replacement_iter)
        .map(|((val, re), replacement)| match (val, re, replacement) {
            (Some(val), Some(re), Some(replacement)) => Ok(Some(re?.replace_all(val, replacement))),
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

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
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}
