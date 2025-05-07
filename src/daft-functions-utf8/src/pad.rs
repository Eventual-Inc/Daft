use std::iter::RepeatN;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::DataArray,
    prelude::{AsArrow, DaftIntegerType, DaftNumericType, DataType, FullNull, Utf8Array},
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use itertools::Itertools;
use num_traits::NumCast;

use crate::utils::create_broadcasted_str_iter;

#[derive(Debug, Clone, Copy)]
pub enum PadPlacement {
    Left,
    Right,
}

pub(crate) fn series_pad(
    s: &Series,
    length: &Series,
    pad: &Series,
    placement: PadPlacement,
) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
       pad.with_utf8_array(|pad_arr| {
           if length.data_type().is_integer() {
               with_match_integer_daft_types!(length.data_type(), |$T| {
                   Ok(pad_impl(arr, length.downcast::<<$T as DaftDataType>::ArrayType>()?, pad_arr, placement)?.into_series())
               })
           } else if length.data_type().is_null() {
               Ok(s.clone())
           } else {
               Err(DaftError::TypeError(format!(
                   "Lpad not implemented for length type {}",
                   length.data_type()
               )))
           }

       })
   })
}

fn pad_impl<I>(
    arr: &Utf8Array,
    length: &DataArray<I>,
    padchar: &Utf8Array,
    placement: PadPlacement,
) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord,
{
    let input_length = arr.len();
    let other_lengths = [length.len(), padchar.len()];

    // Parse the expected `result_len` from the length of the input and other arguments
    let expected_size = if input_length == 0 {
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
        return Err(DaftError::ValueError(format!(
            "Inputs have invalid lengths: {invalid_length_str}"
        )))?;
    };

    // check if any array has all nulls
    if arr.null_count() == arr.len()
        || length.null_count() == length.len()
        || padchar.null_count() == padchar.len()
    {
        return Ok(Utf8Array::full_null(
            arr.name(),
            &DataType::Utf8,
            expected_size,
        ));
    }

    let placement_fn = match placement {
        PadPlacement::Left => {
            |fillchar: RepeatN<char>, val: &str| -> String { fillchar.chain(val.chars()).collect() }
        }
        PadPlacement::Right => {
            |fillchar: RepeatN<char>, val: &str| -> String { val.chars().chain(fillchar).collect() }
        }
    };

    let self_iter = create_broadcasted_str_iter(arr, expected_size);
    let padchar_iter = create_broadcasted_str_iter(padchar, expected_size);
    let result: Utf8Array = match length.len() {
        1 => {
            let len = length.get(0).unwrap();
            let len: usize = NumCast::from(len).ok_or_else(|| {
                DaftError::ComputeError(format!(
                    "Error in pad: failed to cast length as usize {len}"
                ))
            })?;
            let arrow_result = self_iter
                .zip(padchar_iter)
                .map(|(val, padchar)| match (val, padchar) {
                    (Some(val), Some(padchar)) => {
                        Ok(Some(pad_str(val, len, padchar, placement_fn)?))
                    }
                    _ => Ok(None),
                })
                .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
        _ => {
            let length_iter = length.as_arrow().iter();
            let arrow_result = self_iter
                .zip(length_iter)
                .zip(padchar_iter)
                .map(|((val, len), padchar)| match (val, len, padchar) {
                    (Some(val), Some(len), Some(padchar)) => {
                        let len: usize = NumCast::from(*len).ok_or_else(|| {
                            DaftError::ComputeError(format!(
                                "Error in pad: failed to cast length as usize {len}"
                            ))
                        })?;
                        Ok(Some(pad_str(val, len, padchar, placement_fn)?))
                    }
                    _ => Ok(None),
                })
                .collect::<DaftResult<arrow2::array::Utf8Array<i64>>>()?;

            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
    };

    assert_eq!(result.len(), expected_size);
    Ok(result)
}

fn pad_str(
    val: &str,
    length: usize,
    fillchar: &str,
    placement_fn: impl Fn(RepeatN<char>, &str) -> String,
) -> DaftResult<String> {
    if val.chars().count() >= length {
        return Ok(val.chars().take(length).collect());
    }
    let fillchar = if fillchar.is_empty() {
        return Err(DaftError::ComputeError(
            "Error in pad: empty pad character".to_string(),
        ));
    } else if fillchar.chars().count() > 1 {
        return Err(DaftError::ComputeError(format!(
            "Error in pad: {} is not a valid pad character",
            fillchar.len()
        )));
    } else {
        fillchar.chars().next().unwrap()
    };
    let fillchar = std::iter::repeat_n(fillchar, length.saturating_sub(val.chars().count()));
    Ok(placement_fn(fillchar, val))
}
