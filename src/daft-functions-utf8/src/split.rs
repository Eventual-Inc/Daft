use std::sync::Arc;

use arrow::array::{LargeStringBuilder, NullBufferBuilder, OffsetBufferBuilder};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ListArray,
    prelude::{AsArrow, DataType, Field, FromArrow, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Split;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RegexpSplit;

#[typetag::serde]
impl ScalarUDF for Split {
    fn name(&self) -> &'static str {
        "split"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "delimiter", |s, pat| series_split(s, pat, false))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::List(Box::new(DataType::Utf8)),
        )
    }

    fn docstring(&self) -> &'static str {
        "Splits a string into substrings based on a delimiter."
    }
}
#[typetag::serde]
impl ScalarUDF for RegexpSplit {
    fn name(&self) -> &'static str {
        "regexp_split"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "delimiter", |s, pat| series_split(s, pat, true))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::List(Box::new(DataType::Utf8)),
        )
    }

    fn docstring(&self) -> &'static str {
        "Splits a string into substrings based on a regular expression pattern."
    }
}

#[must_use]
pub fn split(input: ExprRef, pattern: ExprRef, regex: bool) -> ExprRef {
    let inputs = vec![input, pattern];
    if regex {
        ScalarFn::builtin(RegexpSplit, inputs).into()
    } else {
        ScalarFn::builtin(Split, inputs).into()
    }
}

fn series_split(s: &Series, pattern: &Series, regex: bool) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern
            .with_utf8_array(|pattern_arr| Ok(split_impl(arr, pattern_arr, regex)?.into_series()))
    })
}

fn split_impl(arr: &Utf8Array, pattern: &Utf8Array, regex: bool) -> DaftResult<ListArray> {
    let (is_full_null, expected_size) = parse_inputs(arr, &[pattern])
        .map_err(|e| DaftError::ValueError(format!("Error in split: {e}")))?;
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

    let arr_arrow = arr.as_arrow()?;
    let buffer_len = arr_arrow.values().len();
    // This will overallocate by pattern_len * N_i, where N_i is the number of pattern occurrences in the ith string in arr_iter.
    let mut splits = LargeStringBuilder::with_capacity(expected_size, buffer_len);
    let mut offsets = OffsetBufferBuilder::<i64>::new(expected_size);
    let mut null_builder = NullBufferBuilder::new(expected_size);

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);
    match (regex, pattern.len()) {
        (true, 1) => {
            let regex = regex::Regex::new(pattern.get(0).unwrap());
            let regex_iter = std::iter::repeat_n(Some(regex), expected_size);
            split_array_on_regex(
                arr_iter,
                regex_iter,
                &mut splits,
                &mut offsets,
                &mut null_builder,
            )?;
        }
        (true, _) => {
            let regex_iter = pattern.into_iter().map(|pat| pat.map(regex::Regex::new));
            split_array_on_regex(
                arr_iter,
                regex_iter,
                &mut splits,
                &mut offsets,
                &mut null_builder,
            )?;
        }
        (false, _) => {
            let pattern_iter = create_broadcasted_str_iter(pattern, expected_size);
            split_array_on_literal(
                arr_iter,
                pattern_iter,
                &mut splits,
                &mut offsets,
                &mut null_builder,
            )?;
        }
    }
    // Build the arrow-rs LargeListArray
    let splits_array = Arc::new(splits.finish());
    let offsets_buffer = offsets.finish();
    let nulls = null_builder.finish();

    let list_field = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::LargeUtf8,
        true,
    ));
    let arrow_list =
        arrow::array::LargeListArray::new(list_field, offsets_buffer, splits_array, nulls);

    // Convert to Daft ListArray
    let result_field = Field::new(arr.name(), DataType::List(Box::new(DataType::Utf8)));
    let result = ListArray::from_arrow(result_field, Arc::new(arrow_list))?;

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

fn split_array_on_regex<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    regex_iter: impl Iterator<Item = Option<Result<regex::Regex, regex::Error>>>,
    splits: &mut LargeStringBuilder,
    offsets: &mut OffsetBufferBuilder<i64>,
    null_builder: &mut NullBufferBuilder,
) -> DaftResult<()> {
    for (val, re) in arr_iter.zip(regex_iter) {
        let mut num_splits = 0;
        match (val, re) {
            (Some(val), Some(re)) => {
                for split in re?.split(val) {
                    splits.append_value(split);
                    num_splits += 1;
                }
                null_builder.append_non_null();
            }
            (_, _) => {
                null_builder.append_null();
            }
        }
        offsets.push_length(num_splits);
    }
    Ok(())
}
fn split_array_on_literal<'a>(
    arr_iter: impl Iterator<Item = Option<&'a str>>,
    pattern_iter: impl Iterator<Item = Option<&'a str>>,
    splits: &mut LargeStringBuilder,
    offsets: &mut OffsetBufferBuilder<i64>,
    null_builder: &mut NullBufferBuilder,
) -> DaftResult<()> {
    for (val, pat) in arr_iter.zip(pattern_iter) {
        let mut num_splits = 0;
        match (val, pat) {
            (Some(val), Some(pat)) => {
                for split in val.split(pat) {
                    splits.append_value(split);
                    num_splits += 1;
                }
                null_builder.append_non_null();
            }
            (_, _) => {
                null_builder.append_null();
            }
        }
        offsets.push_length(num_splits);
    }
    Ok(())
}
