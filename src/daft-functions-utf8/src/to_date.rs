use arrow2::temporal_conversions;
use chrono::Datelike;
use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, DateArray, Field, Int32Array, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::utils::binary_utf8_to_field;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToDate;

#[typetag::serde]
impl ScalarUDF for ToDate {
    fn name(&self) -> &'static str {
        "to_date"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, ValueError: "Expected 2 input args, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "format"))?;
        ensure!(pattern.len() == 1, ValueError: "Expected scalar value for pattern, got {}", pattern.len());
        let pattern = pattern.utf8()?.get(0).unwrap();
        input.with_utf8_array(|arr| Ok(to_date_impl(arr, pattern)?.into_series()))
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "format",
            DataType::is_string,
            self.name(),
            DataType::Date,
        )
    }

    fn docstring(&self) -> &'static str {
        "Parses the string as a date using the specified format."
    }
}

#[must_use]
pub fn to_date(input: ExprRef, format: ExprRef) -> ExprRef {
    ScalarFunction::new(ToDate, vec![input, format]).into()
}

fn to_date_impl(arr: &Utf8Array, format: &str) -> DaftResult<DateArray> {
    let len = arr.len();
    let arr_iter = arr.as_arrow().iter();

    let arrow_result = arr_iter
        .map(|val| match val {
            Some(val) => {
                let date = chrono::NaiveDate::parse_from_str(val, format).map_err(|e| {
                    DaftError::ComputeError(format!(
                        "Error in to_date: failed to parse date {val} with format {format} : {e}"
                    ))
                })?;
                Ok(Some(
                    date.num_days_from_ce() - temporal_conversions::EPOCH_DAYS_FROM_CE,
                ))
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<arrow2::array::Int32Array>>()?;

    let result = Int32Array::from((arr.name(), Box::new(arrow_result)));
    let result = DateArray::new(Field::new(arr.name(), DataType::Date), result);
    assert_eq!(result.len(), len);
    Ok(result)
}
