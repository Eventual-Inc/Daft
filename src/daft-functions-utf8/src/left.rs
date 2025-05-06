use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    array::DataArray,
    prelude::{
        AsArrow, DaftIntegerType, DaftNumericType, DataType, Field, FullNull, Schema, Utf8Array,
    },
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use num_traits::NumCast;
use serde::{Deserialize, Serialize};

use crate::utils::{create_broadcasted_str_iter, parse_inputs};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Left;

#[typetag::serde]
impl ScalarUDF for Left {
    fn name(&self) -> &'static str {
        "left"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let n_chars = inputs.required((1, "n_chars"))?;
        series_left(input, n_chars)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            TypeError: "Expected 2 input args, got {}", inputs.len()
        );

        let data_field = inputs.required((0, "input"))?.to_field(schema)?;

        let n_chars = inputs.required((1, "n_chars"))?.to_field(schema)?;

        ensure!(
            n_chars.dtype.is_integer() && data_field.dtype == DataType::Utf8,
            TypeError: "Expects inputs to left to be utf8 and integer, but received {} and {}", data_field.dtype, n_chars.dtype
        );

        Ok(data_field)
    }
}

#[must_use]
pub fn left(input: ExprRef, nchars: ExprRef) -> ExprRef {
    ScalarFunction::new(Left {}, vec![input, nchars]).into()
}

pub fn series_left(s: &Series, nchars: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        if nchars.data_type().is_integer() {
            with_match_integer_daft_types!(nchars.data_type(), |$T| {
                Ok(left_impl(arr, nchars.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
            })
        } else if nchars.data_type().is_null() {
            Ok(s.clone())
        } else {
            Err(DaftError::TypeError(format!(
                "Left not implemented for nchar type {}",
                nchars.data_type()
            )))
        }
    })
}

fn left_impl<I>(arr: &Utf8Array, nchars: &DataArray<I>) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord,
{
    let (is_full_null, expected_size) = parse_inputs(arr, &[nchars])
        .map_err(|e| DaftError::ValueError(format!("Error in left: {e}")))?;
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

    fn left_most_chars(val: &str, n: usize) -> &str {
        if n == 0 || val.is_empty() {
            ""
        } else {
            val.char_indices().nth(n).map_or(val, |(i, _)| &val[..i])
        }
    }

    let self_iter = create_broadcasted_str_iter(arr, expected_size);
    let result: Utf8Array = match nchars.len() {
        1 => {
            let n = nchars.get(0).unwrap();
            let n: usize = NumCast::from(n).ok_or_else(|| {
                DaftError::ComputeError(format!("Error in left: failed to cast rhs as usize {n}"))
            })?;
            let arrow_result = self_iter
                .map(|val| Some(left_most_chars(val?, n)))
                .collect::<arrow2::array::Utf8Array<i64>>();
            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
        _ => {
            let arrow_result = self_iter
                .zip(nchars.as_arrow().iter())
                .map(|(val, n)| match (val, n) {
                    (Some(val), Some(nchar)) => {
                        let nchar: usize = NumCast::from(*nchar).ok_or_else(|| {
                            DaftError::ComputeError(format!(
                                "Error in left: failed to cast rhs as usize {nchar}"
                            ))
                        })?;
                        Ok(Some(left_most_chars(val, nchar)))
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
