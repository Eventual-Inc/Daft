use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpExtract;

#[typetag::serde]
impl ScalarUDF for RegexpExtract {
    fn name(&self) -> &'static str {
        "extract"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["regexp_extract"]
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(
            inputs.len() == 2 || inputs.len() == 3,
            ComputeError: "Expected 2 or 3 input args, got {}",
            inputs.len()
        );

        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        let opt_index = inputs.optional((2, "index"))?;
        let index = if let Some(index) = opt_index {
            ensure!(index.len() == 1, "Expected scalar value for index");
            index.cast(&DataType::UInt64)?.u64()?.get(0).unwrap()
        } else {
            0
        };

        input.with_utf8_array(|arr| {
            pattern.with_utf8_array(|pattern_arr| {
                extract_impl(arr, pattern_arr, index as _).map(IntoSeries::into_series)
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2 || inputs.len() == 3,
            SchemaMismatch: "Expected 2 or 3 input args, got {}",
            inputs.len()
        );
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "Expects 'input' to be utf8, but received {}", input.dtype);

        let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
        ensure!(pattern.dtype == DataType::Utf8, TypeError: "Expects 'pattern' to be utf8, but received {}", pattern.dtype);

        if let Some(index) = inputs.optional((2, "index"))? {
            let index = index.to_field(schema)?;
            ensure!(index.dtype.is_numeric() && !index.dtype.is_floating(), TypeError: "Expects 'index' to be numeric, but received {}", index.dtype);
        }

        Ok(Field::new(input.name, DataType::Utf8))
    }
}

#[must_use]
pub fn regexp_extract(input: ExprRef, pattern: ExprRef, index: ExprRef) -> ExprRef {
    ScalarFunction::new(RegexpExtract, vec![input, pattern, index]).into()
}

fn extract_impl(s: &Utf8Array, pattern: &Utf8Array, index: usize) -> DaftResult<Utf8Array> {
    let (is_full_null, expected_size) = parse_inputs(s, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in extract: {e}")))?;
    if is_full_null {
        return Ok(Utf8Array::full_null(
            s.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(Utf8Array::empty(s.name(), &DataType::Utf8));
    }

    let self_iter = create_broadcasted_str_iter(s, expected_size);
    let result = match pattern.len() {
        1 => {
            let regex = regex::Regex::new(pattern.get(0).unwrap());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            regex_extract_first_match(self_iter, regex_iter, index, s.name())?
        }
        _ => {
            let regex_iter = pattern
                .as_arrow()
                .iter()
                .map(|pat| pat.map(regex::Regex::new));
            regex_extract_first_match(self_iter, regex_iter, index, s.name())?
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

fn regex_extract_first_match<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    index: usize,
    name: &str,
) -> DaftResult<Utf8Array> {
    let arrow_result = arr_iter
        .zip(regex_iter)
        .map(|(val, re)| match (val, re) {
            (Some(val), Some(re)) => {
                // https://docs.rs/regex/latest/regex/struct.Regex.html#method.captures
                // regex::find is faster than regex::captures but only returns the full match, not the capture groups.
                // So, use regex::find if index == 0, otherwise use regex::captures.
                if index == 0 {
                    Ok(re?.find(val).map(|m| m.as_str()))
                } else {
                    Ok(re?
                        .captures(val)
                        .and_then(|captures| captures.get(index))
                        .map(|m| m.as_str()))
                }
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>();

    Ok(Utf8Array::from((name, Box::new(arrow_result?))))
}
