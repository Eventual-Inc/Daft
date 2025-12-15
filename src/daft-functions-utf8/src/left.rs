use common_error::{DaftError, DaftResult};
use daft_core::{
    array::DataArray,
    prelude::{DaftIntegerType, DaftNumericType, DataType, Field, FullNull, Schema, Utf8Array},
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::NumCast;
use serde::{Deserialize, Serialize};

use crate::utils::{
    binary_utf8_evaluate, binary_utf8_to_field, create_broadcasted_str_iter, parse_inputs,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Left;

#[typetag::serde]
impl ScalarUDF for Left {
    fn name(&self) -> &'static str {
        "left"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "n", |s, nchars| {
            s.with_utf8_array(|arr| {
            if nchars.data_type().is_integer() {
                with_match_integer_daft_types!(nchars.data_type(), |$T| {
                    Ok(left_impl(arr, nchars.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            } else if nchars.data_type().is_null() {
                Ok(s.clone())
            } else {
                Err(DaftError::ValueError(format!(
                    "Left not implemented for nchar type {}",
                    nchars.data_type()
                )))
            }
        })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "n",
            DataType::is_integer,
            self.name(),
            DataType::Utf8,
        )
    }

    fn docstring(&self) -> &'static str {
        "Returns the leftmost n characters of each string in the input series."
    }
}

#[must_use]
pub fn left(input: ExprRef, nchars: ExprRef) -> ExprRef {
    ScalarFn::builtin(Left {}, vec![input, nchars]).into()
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
                .collect::<daft_arrow::array::Utf8Array<i64>>();
            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
        _ => {
            let arrow_result = self_iter
                .zip(nchars.into_iter())
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
                .collect::<DaftResult<daft_arrow::array::Utf8Array<i64>>>()?;

            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}
