use common_error::{ensure, DaftResult};
use daft_core::{
    array::DataArray,
    prelude::{AsArrow, DaftPhysicalType, DataType, Field, Schema, Utf8Array},
    series::Series,
};
use daft_dsl::{functions::FunctionArgs, ExprRef};
use itertools::Itertools;

pub(crate) enum BroadcastedStrIter<'a> {
    Repeat(std::iter::RepeatN<Option<&'a str>>),
    NonRepeat(
        arrow2::bitmap::utils::ZipValidity<
            &'a str,
            arrow2::array::ArrayValuesIter<'a, arrow2::array::Utf8Array<i64>>,
            arrow2::bitmap::utils::BitmapIter<'a>,
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
        BroadcastedStrIter::NonRepeat(arr.as_arrow().iter())
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
    fn_name: &'static str,
    return_dtype: DataType,
) -> DaftResult<Field> {
    ensure!(inputs.len() == 2, SchemaMismatch: "'{fn_name}' expects 2 arguments");

    let input = inputs.required((0, "input"))?.to_field(schema)?;

    ensure!(input.dtype == DataType::Utf8, TypeError: "input must be of type Utf8");

    let pattern = inputs.required((1, arg_name))?.to_field(schema)?;
    ensure!(
        pattern.dtype == DataType::Utf8,
        TypeError: "{arg_name} must be of type Utf8"
    );
    Ok(Field::new(input.name, return_dtype))
}
