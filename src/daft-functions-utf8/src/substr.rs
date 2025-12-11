#![allow(deprecated, reason = "arrow2 migration")]
use std::iter;

use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::DataArray,
    prelude::{
        AsArrow, DaftIntegerType, DaftNumericType, DataType, Field, FullNull, Schema, Utf8Array,
    },
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::NumCast;
use serde::{Deserialize, Serialize};

use crate::utils::{BroadcastedStrIter, create_broadcasted_str_iter, parse_inputs};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Substr;

#[derive(FunctionArgs)]
struct SubstrArgs<T> {
    input: T,
    start: T,
    #[arg(optional)]
    length: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for Substr {
    fn name(&self) -> &'static str {
        "substr"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let SubstrArgs {
            input: data,
            start,
            length,
        } = inputs.try_into()?;
        let length =
            length.unwrap_or_else(|| Series::full_null("length", &DataType::Null, data.len()));

        data.with_utf8_array(|arr| {
                if start.data_type().is_integer() {
                    with_match_integer_daft_types!(start.data_type(), |$T| {
                        if length.data_type().is_integer() {
                            with_match_integer_daft_types!(length.data_type(), |$U| {
                                Ok(substr_impl(arr, start.downcast::<<$T as DaftDataType>::ArrayType>()?, Some(length.downcast::<<$U as DaftDataType>::ArrayType>()?))?.into_series())
                            })
                        } else if length.data_type().is_null() {
                            Ok(substr_impl(arr, start.downcast::<<$T as DaftDataType>::ArrayType>()?, None::<&DataArray<Int8Type>>)?.into_series())
                        } else {
                            Err(DaftError::TypeError(format!(
                                "Substr not implemented for length type {}",
                                length.data_type()
                            )))
                        }
                })
                } else if start.data_type().is_null() {
                    Ok(data.clone())
                } else {
                    Err(DaftError::TypeError(format!(
                        "Substr not implemented for start type {}",
                        start.data_type()
                    )))
                }
            })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let SubstrArgs {
            input,
            start,
            length,
        } = inputs.try_into()?;
        let data = input.to_field(schema)?;
        let start = start.to_field(schema)?;

        ensure!(data.dtype.is_string(), TypeError: "Expected input to be utf8 but received {}", data.dtype);
        ensure!(start.dtype.is_integer(), TypeError: "Expected start to be an integer but received {}", start.dtype);

        if let Some(length) = length {
            let length = length.to_field(schema)?;
            ensure!(length.dtype.is_null_or(DataType::is_integer), TypeError: "Expected length to be an integer but received {}", length.dtype);
        }

        Ok(data)
    }
    fn docstring(&self) -> &'static str {
        "Returns a substring of the input string starting at the specified index and with the specified length."
    }
}

#[must_use]
pub fn substr(input: ExprRef, start: ExprRef, length: ExprRef) -> ExprRef {
    ScalarFn::builtin(Substr {}, vec![input, start, length]).into()
}

fn substr_impl<I, J>(
    arr: &Utf8Array,
    start: &DataArray<I>,
    length: Option<&DataArray<J>>,
) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord,
    J: DaftIntegerType,
    <J as DaftNumericType>::Native: Ord,
{
    let name = arr.name();
    let (is_full_null, expected_size) = parse_inputs(arr, &[start])
        .map_err(|e| DaftError::ValueError(format!("Error in substr: {e}")))?;

    if is_full_null {
        return Ok(Utf8Array::full_null(name, &DataType::Utf8, expected_size));
    }

    let arr_iter = create_broadcasted_str_iter(arr, expected_size);

    let (length_repeat, length_iter) = match length {
        Some(length) => {
            if length.len() != 1 && length.len() != expected_size {
                return Err(DaftError::ValueError(
                    "Inputs have invalid lengths: length".to_string(),
                ));
            }

            match length.len() {
                1 => {
                    let length_repeat: Result<Option<usize>, ()> = if length.null_count() == 1 {
                        Ok(None)
                    } else {
                        let val = length.get(0).unwrap();
                        let val: usize = NumCast::from(val).ok_or_else(|| {
                            DaftError::ComputeError(format!(
                                "Error in substr: failed to cast length as usize {val}"
                            ))
                        })?;

                        Ok(Some(val))
                    };

                    let length_repeat = iter::repeat_n(length_repeat, expected_size);
                    (Some(length_repeat), None)
                }
                _ => {
                    let length_iter = length.as_arrow2().iter().map(|l| match l {
                        Some(l) => {
                            let l: usize = NumCast::from(*l).ok_or_else(|| {
                                DaftError::ComputeError(format!(
                                    "Error in repeat: failed to cast length as usize {l}"
                                ))
                            })?;
                            let result: Result<Option<usize>, DaftError> = Ok(Some(l));
                            result
                        }
                        None => Ok(None),
                    });
                    (None, Some(length_iter))
                }
            }
        }
        None => {
            let none_value_iter = iter::repeat_n(Ok(None), expected_size);
            (Some(none_value_iter), None)
        }
    };

    let (start_repeat, start_iter) = match start.len() {
        1 => {
            let start_repeat = start.get(0).unwrap();
            let start_repeat: usize = NumCast::from(start_repeat).ok_or_else(|| {
                DaftError::ComputeError(format!(
                    "Error in substr: failed to cast start as usize {start_repeat}"
                ))
            })?;
            let start_repeat: Result<Option<usize>, ()> = Ok(Some(start_repeat));
            let start_repeat = iter::repeat_n(start_repeat, expected_size);
            (Some(start_repeat), None)
        }
        _ => {
            let start_iter = start.as_arrow2().iter().map(|s| match s {
                Some(s) => {
                    let s: usize = NumCast::from(*s).ok_or_else(|| {
                        DaftError::ComputeError(format!(
                            "Error in repeat: failed to cast length as usize {s}"
                        ))
                    })?;
                    let result: Result<Option<usize>, DaftError> = Ok(Some(s));
                    result
                }
                None => Ok(None),
            });
            (None, Some(start_iter))
        }
    };

    match (start_iter, start_repeat, length_iter, length_repeat) {
        (Some(start_iter), None, Some(length_iter), None) => {
            substr_compute_result(name, arr_iter, start_iter, length_iter)
        }
        (Some(start_iter), None, None, Some(length_repeat)) => {
            substr_compute_result(name, arr_iter, start_iter, length_repeat)
        }
        (None, Some(start_repeat), Some(length_iter), None) => {
            substr_compute_result(name, arr_iter, start_repeat, length_iter)
        }
        (None, Some(start_repeat), None, Some(length_repeat)) => {
            substr_compute_result(name, arr_iter, start_repeat, length_repeat)
        }

        _ => Err(DaftError::ComputeError(
            "Start and length parameters are empty".to_string(),
        )),
    }
}
fn substr_compute_result<I, U, E, R>(
    name: &str,
    iter: BroadcastedStrIter,
    start: I,
    length: U,
) -> DaftResult<Utf8Array>
where
    I: Iterator<Item = Result<Option<usize>, E>>,
    U: Iterator<Item = Result<Option<usize>, R>>,
{
    let arrow_result = iter
        .zip(start)
        .zip(length)
        .map(|((val, s), l)| {
            let s = match s {
                Ok(s) => s,
                Err(_) => {
                    return Err(DaftError::ComputeError(
                        "Error in repeat: failed to cast length as usize".to_string(),
                    ));
                }
            };
            let l = match l {
                Ok(l) => l,
                Err(_) => {
                    return Err(DaftError::ComputeError(
                        "Error in repeat: failed to cast length as usize".to_string(),
                    ));
                }
            };

            match (val, s, l) {
                (Some(val), Some(s), Some(l)) => Ok(substring(val, s, Some(l))),
                (Some(val), Some(s), None) => Ok(substring(val, s, None)),
                _ => Ok(None),
            }
        })
        .collect::<DaftResult<daft_arrow::array::Utf8Array<i64>>>()?;

    Ok(Utf8Array::from((name, Box::new(arrow_result))))
}

fn substring(s: &str, start: usize, len: Option<usize>) -> Option<&str> {
    let mut char_indices = s.char_indices();

    if let Some((start_pos, _)) = char_indices.nth(start) {
        let len = match len {
            Some(len) => {
                if len == 0 {
                    return None;
                }

                len
            }
            None => {
                return Some(&s[start_pos..]);
            }
        };

        let end_pos = char_indices
            .nth(len.saturating_sub(1))
            .map_or(s.len(), |(idx, _)| idx);

        Some(&s[start_pos..end_pos])
    } else {
        None
    }
}
