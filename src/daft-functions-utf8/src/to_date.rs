use chrono::Datelike;
use common_error::{DaftError, DaftResult, ensure};
use daft_arrow::temporal_conversions;
use daft_core::{
    prelude::{AsArrow, DataType, DateArray, Field, Int32Array, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::binary_utf8_to_field;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToDate;

#[typetag::serde]
impl ScalarUDF for ToDate {
    fn name(&self) -> &'static str {
        "to_date"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 2, ValueError: "Expected 2 input args, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "format"))?;
        ensure!(pattern.len() == 1, ValueError: "Expected scalar value for pattern, got {}", pattern.len());
        let pattern = pattern.utf8()?.get(0).unwrap();
        input.with_utf8_array(|arr| Ok(to_date_impl(arr, pattern)?.into_series()))
    }

    fn get_return_field(
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
    ScalarFn::builtin(ToDate, vec![input, format]).into()
}

fn to_date_impl(arr: &Utf8Array, format: &str) -> DaftResult<DateArray> {
    let len = arr.len();
    let arr_iter = arr.as_arrow2().iter();

    #[allow(deprecated, reason = "Int32Array doesn't have FromIterator yet")]
    let arrow_result = arr_iter
        .map(|val| match val {
            Some(val) => {
                let date = chrono::NaiveDate::parse_from_str(val, format).map_err(|e| {
                    DaftError::ComputeError(format!(
                        "Error in to_date: failed to parse date {val} with format {format} : {e}"
                    ))
                })?;
                Ok(Some(
                    date.num_days_from_ce() - (temporal_conversions::UNIX_EPOCH_DAY as i32),
                ))
            }
            _ => Ok(None),
        })
        .collect::<DaftResult<daft_arrow::array::Int32Array>>()?;

    #[allow(deprecated, reason = "Int32Array doesn't have FromIterator yet")]
    let result = Int32Array::from((arr.name(), Box::new(arrow_result)));
    let result = DateArray::new(Field::new(arr.name(), DataType::Date), result);
    assert_eq!(result.len(), len);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::Utf8Array;

    use super::*;

    #[test]
    fn test_to_date_with_values() {
        let arr = Utf8Array::from_iter(
            "dates",
            vec![Some("2023-01-15"), Some("2020-06-30"), Some("1999-12-31")].into_iter(),
        );
        let format = "%Y-%m-%d";

        let result = to_date_impl(&arr, format).unwrap();

        assert_eq!(result.len(), 3);
        // 2023-01-15 is 19372 days after Unix epoch (1970-01-01)
        assert_eq!(result.get(0), Some(19372));
        // 2020-06-30 is 18443 days after Unix epoch
        assert_eq!(result.get(1), Some(18443));
        // 1999-12-31 is 10956 days after Unix epoch
        assert_eq!(result.get(2), Some(10956));
    }

    #[test]
    fn test_to_date_with_nulls() {
        let arr = Utf8Array::from_iter(
            "dates",
            vec![Some("2023-01-15"), None, Some("2020-06-30")].into_iter(),
        );
        let format = "%Y-%m-%d";

        let result = to_date_impl(&arr, format).unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.get(0).is_some());
        assert!(result.get(1).is_none());
        assert!(result.get(2).is_some());
    }

    #[test]
    fn test_to_date_different_format() {
        let arr = Utf8Array::from_iter(
            "dates",
            vec![Some("15/01/2023"), Some("30/06/2020")].into_iter(),
        );
        let format = "%d/%m/%Y";

        let result = to_date_impl(&arr, format).unwrap();

        assert_eq!(result.len(), 2);
        // Should parse as same dates as above
        assert_eq!(result.get(0), Some(19372)); // 2023-01-15
        assert_eq!(result.get(1), Some(18443)); // 2020-06-30
    }

    #[test]
    fn test_to_date_invalid_format_error() {
        let arr = Utf8Array::from_iter("dates", vec![Some("not-a-date")].into_iter());
        let format = "%Y-%m-%d";

        let result = to_date_impl(&arr, format);

        assert!(result.is_err());
    }

    #[test]
    fn test_to_date_unix_epoch() {
        let arr = Utf8Array::from_iter("dates", vec![Some("1970-01-01")].into_iter());
        let format = "%Y-%m-%d";

        let result = to_date_impl(&arr, format).unwrap();

        // Unix epoch should be day 0
        assert_eq!(result.get(0), Some(0));
    }

    #[test]
    fn test_to_date_before_unix_epoch() {
        let arr = Utf8Array::from_iter("dates", vec![Some("1969-12-31")].into_iter());
        let format = "%Y-%m-%d";

        let result = to_date_impl(&arr, format).unwrap();

        // Day before Unix epoch should be -1
        assert_eq!(result.get(0), Some(-1));
    }
}
