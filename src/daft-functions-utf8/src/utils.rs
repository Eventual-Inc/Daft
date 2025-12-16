#![allow(deprecated, reason = "arrow2 migration")]
use std::borrow::Cow;

use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::DataArray,
    prelude::{
        AsArrow, BooleanArray, DaftPhysicalType, DataType, Field, FullNull, Schema, Utf8Array,
    },
    series::Series,
};
use daft_dsl::{ExprRef, functions::FunctionArgs};
use itertools::Itertools;

pub(crate) enum BroadcastedStrIter<'a> {
    Repeat(std::iter::RepeatN<Option<&'a str>>),
    NonRepeat(
        daft_arrow::bitmap::utils::ZipValidity<
            &'a str,
            daft_arrow::array::ArrayValuesIter<'a, daft_arrow::array::Utf8Array<i64>>,
            daft_arrow::bitmap::utils::BitmapIter<'a>,
        >,
    ),
}

impl<'a> Iterator for BroadcastedStrIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedStrIter::Repeat(iter) => iter.next(),
            BroadcastedStrIter::NonRepeat(iter) => iter.next(),
        }
    }
}

pub(crate) fn create_broadcasted_str_iter(arr: &Utf8Array, len: usize) -> BroadcastedStrIter<'_> {
    if arr.len() == 1 {
        BroadcastedStrIter::Repeat(std::iter::repeat_n(arr.get(0), len))
    } else {
        BroadcastedStrIter::NonRepeat(arr.as_arrow2().iter())
    }
}

pub(crate) trait Utf8ArrayUtils {
    fn unary_broadcasted_op<ScalarKernel>(&self, operation: ScalarKernel) -> DaftResult<Utf8Array>
    where
        ScalarKernel: Fn(&str) -> Cow<'_, str>;

    fn binary_broadcasted_compare<ScalarKernel>(
        &self,
        other: &Self,
        operation: ScalarKernel,
        op_name: &str,
    ) -> DaftResult<BooleanArray>
    where
        ScalarKernel: Fn(&str, &str) -> DaftResult<bool>;
}

impl Utf8ArrayUtils for Utf8Array {
    fn unary_broadcasted_op<ScalarKernel>(&self, operation: ScalarKernel) -> DaftResult<Self>
    where
        ScalarKernel: Fn(&str) -> Cow<'_, str>,
    {
        let self_arrow = self.as_arrow2();
        let arrow_result = self_arrow
            .iter()
            .map(|val| Some(operation(val?)))
            .collect::<daft_arrow::array::Utf8Array<i64>>()
            .with_validity(self_arrow.validity().cloned());
        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }
    fn binary_broadcasted_compare<ScalarKernel>(
        &self,
        other: &Self,
        operation: ScalarKernel,
        op_name: &str,
    ) -> DaftResult<BooleanArray>
    where
        ScalarKernel: Fn(&str, &str) -> DaftResult<bool>,
    {
        let (is_full_null, expected_size) = parse_inputs(self, &[other])
            .map_err(|e| DaftError::ValueError(format!("Error in {op_name}: {e}")))?;
        if is_full_null {
            return Ok(BooleanArray::full_null(
                self.name(),
                &DataType::Boolean,
                expected_size,
            ));
        }
        if expected_size == 0 {
            return Ok(BooleanArray::empty(self.name(), &DataType::Boolean));
        }

        let self_iter = create_broadcasted_str_iter(self, expected_size);
        let other_iter = create_broadcasted_str_iter(other, expected_size);
        let arrow_result = self_iter
            .zip(other_iter)
            .map(|(self_v, other_v)| match (self_v, other_v) {
                (Some(self_v), Some(other_v)) => operation(self_v, other_v).map(Some),
                _ => Ok(None),
            })
            .collect::<DaftResult<daft_arrow::array::BooleanArray>>();

        let result = BooleanArray::from((self.name(), arrow_result?));
        assert_eq!(result.len(), expected_size);
        Ok(result)
    }
}

/// Parse inputs for string operations.
/// Returns a tuple of (is_full_null, expected_size).
pub(crate) fn parse_inputs<T>(
    self_arr: &Utf8Array,
    other_arrs: &[&DataArray<T>],
) -> Result<(bool, usize), String>
where
    T: DaftPhysicalType,
{
    let input_length = self_arr.len();
    let other_lengths = other_arrs.iter().map(|arr| arr.len()).collect::<Vec<_>>();

    // Parse the expected `result_len` from the length of the input and other arguments
    let result_len = if input_length == 0 {
        // Empty input: expect empty output
        0
    } else if other_lengths.iter().all(|&x| x == input_length) {
        // All lengths matching: expect non-broadcasted length
        input_length
    } else if let [broadcasted_len] = std::iter::once(&input_length)
        .chain(other_lengths.iter())
        .filter(|&&x| x != 1)
        .sorted()
        .dedup()
        .collect::<Vec<&usize>>()
        .as_slice()
    {
        // All non-unit lengths match: expect broadcast
        **broadcasted_len
    } else {
        let invalid_length_str = itertools::Itertools::join(
            &mut std::iter::once(&input_length)
                .chain(other_lengths.iter())
                .map(|x| x.to_string()),
            ", ",
        );
        return Err(format!("Inputs have invalid lengths: {invalid_length_str}"));
    };

    // check if any array has all nulls
    if other_arrs.iter().any(|arr| arr.null_count() == arr.len())
        || self_arr.null_count() == self_arr.len()
    {
        return Ok((true, result_len));
    }

    Ok((false, result_len))
}

pub(crate) fn binary_utf8_evaluate(
    inputs: daft_dsl::functions::FunctionArgs<Series>,
    arg_name: &'static str,
    f: impl Fn(&Series, &Series) -> DaftResult<Series>,
) -> DaftResult<Series> {
    let input = inputs.required((0, "input"))?;
    if input.data_type().is_null() {
        return Ok(Series::full_null(
            input.name(),
            &DataType::Null,
            input.len(),
        ));
    }
    let arg1 = inputs.required((1, arg_name))?;
    f(input, arg1)
}
pub(crate) fn unary_utf8_evaluate(
    inputs: daft_dsl::functions::FunctionArgs<Series>,
    f: impl Fn(&Series) -> DaftResult<Series>,
) -> DaftResult<Series> {
    let input = inputs.required((0, "input"))?;
    if input.data_type().is_null() {
        return Ok(Series::full_null(
            input.name(),
            &DataType::Null,
            input.len(),
        ));
    }
    f(input)
}
pub(crate) fn unary_utf8_to_field(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
    fn_name: &'static str,
    return_dtype: DataType,
) -> DaftResult<Field> {
    ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
    let input = inputs.required((0, "input"))?.to_field(schema)?;

    if input.dtype.is_null() {
        Ok(Field::new(input.name, DataType::Null))
    } else {
        ensure!(
            input.dtype.is_string(),
            TypeError: "Expects input to '{fn_name}' to be utf8, but received {}", input.dtype
        );

        Ok(Field::new(input.name, return_dtype))
    }
}

pub(crate) fn binary_utf8_to_field(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
    arg_name: &'static str,
    arg_dtype_pattern: impl Fn(&DataType) -> bool,
    fn_name: &'static str,
    return_dtype: DataType,
) -> DaftResult<Field> {
    ensure!(inputs.len() == 2, SchemaMismatch: "'{fn_name}' expects 2 arguments");

    let input = inputs.required((0, "input"))?.to_field(schema)?;

    ensure!(input.dtype == DataType::Utf8, TypeError: "input must be of type Utf8");

    let pattern = inputs.required((1, arg_name))?.to_field(schema)?;
    ensure!(
        arg_dtype_pattern(&pattern.dtype),
        TypeError: "invalid argument type for '{fn_name}'"
    );
    Ok(Field::new(input.name, return_dtype))
}
