use arrow2::array::Array;
use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    array::ListArray,
    prelude::{AsArrow, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpExtractAll;

#[typetag::serde]
impl ScalarUDF for RegexpExtractAll {
    fn name(&self) -> &'static str {
        "extract_all"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["regexp_extract_all"]
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
                extract_all_impl(arr, pattern_arr, index as _).map(IntoSeries::into_series)
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

        Ok(Field::new(
            input.name,
            DataType::List(Box::new(DataType::Utf8)),
        ))
    }

    fn docstring(&self) -> &'static str {
        "Extracts all substrings that match the specified regular expression pattern"
    }
}

#[must_use]
pub fn regexp_extract_all(input: ExprRef, pattern: ExprRef, index: ExprRef) -> ExprRef {
    ScalarFunction::new(RegexpExtractAll, vec![input, pattern, index]).into()
}

fn extract_all_impl(arr: &Utf8Array, pattern: &Utf8Array, index: usize) -> DaftResult<ListArray> {
    let (is_full_null, expected_size) = crate::utils::parse_inputs(arr, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in extract_all: {e}")))?;
    if is_full_null {
        return Ok(ListArray::full_null(
            arr.name(),
            &DataType::List(Box::new(DataType::Utf8)),
            expected_size,
        ));
    }
    if expected_size == 0 {
        return Ok(ListArray::empty(
            arr.name(),
            &DataType::List(Box::new(DataType::Utf8)),
        ));
    }

    let self_iter = crate::utils::create_broadcasted_str_iter(arr, expected_size);
    let result = match pattern.len() {
        1 => {
            let regex = regex::Regex::new(pattern.get(0).unwrap());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            regex_extract_all_matches(self_iter, regex_iter, index, expected_size, arr.name())?
        }
        _ => {
            let regex_iter = pattern
                .as_arrow()
                .iter()
                .map(|pat| pat.map(regex::Regex::new));
            regex_extract_all_matches(self_iter, regex_iter, index, expected_size, arr.name())?
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

fn regex_extract_all_matches<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    index: usize,
    len: usize,
    name: &str,
) -> DaftResult<ListArray> {
    let mut matches = arrow2::array::MutableUtf8Array::new();
    let mut offsets = arrow2::offset::Offsets::new();
    let mut validity = arrow2::bitmap::MutableBitmap::with_capacity(len);

    for (val, re) in arr_iter.zip(regex_iter) {
        let mut num_matches = 0i64;
        match (val, re) {
            (Some(val), Some(re)) => {
                // https://docs.rs/regex/latest/regex/struct.Regex.html#method.captures_iter
                // regex::find_iter is faster than regex::captures_iter but only returns the full match, not the capture groups.
                // So, use regex::find_iter if index == 0, otherwise use regex::captures.
                if index == 0 {
                    for m in re?.find_iter(val) {
                        matches.push(Some(m.as_str()));
                        num_matches += 1;
                    }
                } else {
                    for captures in re?.captures_iter(val) {
                        if let Some(capture) = captures.get(index) {
                            matches.push(Some(capture.as_str()));
                            num_matches += 1;
                        }
                    }
                }
                validity.push(true);
            }
            (_, _) => {
                validity.push(false);
            }
        }
        offsets.try_push(num_matches)?;
    }

    let matches: arrow2::array::Utf8Array<i64> = matches.into();
    let offsets: arrow2::offset::OffsetsBuffer<i64> = offsets.into();
    let validity: Option<arrow2::bitmap::Bitmap> = match validity.unset_bits() {
        0 => None,
        _ => Some(validity.into()),
    };
    let flat_child = Series::try_from(("matches", matches.to_boxed()))?;

    Ok(ListArray::new(
        Field::new(name, DataType::List(Box::new(DataType::Utf8))),
        flat_child,
        offsets,
        validity,
    ))
}

#[must_use]
pub fn utf8_extract_all(input: ExprRef, pattern: ExprRef, index: ExprRef) -> ExprRef {
    ScalarFunction::new(RegexpExtractAll, vec![input, pattern, index]).into()
}
