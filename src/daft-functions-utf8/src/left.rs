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
            let result: Utf8Array = self_iter
                .map(|val| Some(left_most_chars(val?, n)))
                .collect();
            result.rename(arr.name())
        }
        _ => {
            let result: Utf8Array = self_iter
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
                .collect::<DaftResult<Utf8Array>>()?;
            result.rename(arr.name())
        }
    };
    assert_eq!(result.len(), expected_size);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Int64Array;

    use super::*;

    #[test]
    fn test_left_with_values() {
        let arr = Utf8Array::from(("a", vec!["hello", "world", "foo"].as_slice()));
        let nchars = Int64Array::from(("n", vec![2i64, 3i64, 10i64]));
        let result = left_impl(&arr, &nchars).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some("he"));
        assert_eq!(result.get(1), Some("wor"));
        assert_eq!(result.get(2), Some("foo")); // n > len returns full string
    }

    #[test]
    fn test_left_broadcast_nchars() {
        let arr = Utf8Array::from(("a", vec!["hello", "world", "bar"].as_slice()));
        let nchars = Int64Array::from(("n", vec![2i64]));
        let result = left_impl(&arr, &nchars).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.get(0), Some("he"));
        assert_eq!(result.get(1), Some("wo"));
        assert_eq!(result.get(2), Some("ba"));
    }

    #[test]
    fn test_left_with_zero() {
        let arr = Utf8Array::from(("a", vec!["hello"].as_slice()));
        let nchars = Int64Array::from(("n", vec![0i64]));
        let result = left_impl(&arr, &nchars).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get(0), Some(""));
    }
}
