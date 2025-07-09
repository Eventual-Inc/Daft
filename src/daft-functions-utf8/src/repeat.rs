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
pub struct Repeat;

#[typetag::serde]
impl ScalarUDF for Repeat {
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let s = inputs.required((0, "input"))?;
        let n = inputs.required((1, "n"))?;

        s.with_utf8_array(|arr| {
            if n.data_type().is_integer() {
                with_match_integer_daft_types!(n.data_type(), |$T| {
                    Ok(repeat_impl(arr, n.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
                })
            } else if n.data_type().is_null() {
                Ok(s.clone())
            } else {
                Err(DaftError::TypeError(format!(
                    "Repeat not implemented for nchar type {}",
                    n.data_type()
                )))
            }
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            TypeError: "Expected 2 input args, got {}", inputs.len()
        );

        let data_field = inputs.required((0, "input"))?.to_field(schema)?;
        let n = inputs.required((1, "n"))?.to_field(schema)?;

        ensure!(
            n.dtype.is_integer() && data_field.dtype == DataType::Utf8,
            TypeError: "Expects inputs to repeat to be utf8 and integer, but received {} and {}", data_field.dtype, n.dtype
        );

        Ok(data_field)
    }

    fn docstring(&self) -> &'static str {
        "Repeats the string the specified number of times"
    }
}

#[must_use]
pub fn utf8_repeat(input: ExprRef, ntimes: ExprRef) -> ExprRef {
    ScalarFunction::new(Repeat {}, vec![input, ntimes]).into()
}

fn repeat_impl<I>(arr: &Utf8Array, n: &DataArray<I>) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord,
{
    let (is_full_null, expected_size) = parse_inputs(arr, &[n])
        .map_err(|e| DaftError::ValueError(format!("Error in repeat: {e}")))?;
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

    let self_iter = create_broadcasted_str_iter(arr, expected_size);
    let result: Utf8Array = match n.len() {
        1 => {
            let n = n.get(0).unwrap();
            let n: usize = NumCast::from(n).ok_or_else(|| {
                DaftError::ComputeError(format!("Error in repeat: failed to cast rhs as usize {n}"))
            })?;
            let arrow_result = self_iter
                .map(|val| Some(val?.repeat(n)))
                .collect::<arrow2::array::Utf8Array<i64>>();
            Utf8Array::from((arr.name(), Box::new(arrow_result)))
        }
        _ => {
            let arrow_result = self_iter
                .zip(n.as_arrow().iter())
                .map(|(val, n)| match (val, n) {
                    (Some(val), Some(n)) => {
                        let n: usize = NumCast::from(*n).ok_or_else(|| {
                            DaftError::ComputeError(format!(
                                "Error in repeat: failed to cast rhs as usize {n}"
                            ))
                        })?;
                        Ok(Some(val.repeat(n)))
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
