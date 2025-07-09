use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    datatypes::{format_string_has_offset, infer_timeunit_from_format_string},
    prelude::{AsArrow, DataType, Field, Int64Array, Schema, TimeUnit, TimestampArray, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    lit, ExprRef, LiteralValue,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToDatetime;

#[typetag::serde]
impl ScalarUDF for ToDatetime {
    fn name(&self) -> &'static str {
        "to_datetime"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let data = inputs.required((0, "input"))?;
        let format = inputs.required((1, "format"))?;
        ensure!(format.data_type().is_string() && format.len() == 1, ValueError: "format must be a string literal");

        let format = format.utf8().unwrap().get(0).unwrap();

        let tz = if let Some(tz) = inputs.optional("timezone")? {
            if tz.data_type() == &DataType::Null {
                None
            } else {
                ensure!(tz.data_type().is_string() && tz.len() == 1, ValueError: "timezone must be a string literal");
                Some(tz.utf8().unwrap().get(0).unwrap())
            }
        } else {
            None
        };

        data.with_utf8_array(|arr| Ok(to_datetime_impl(arr, format, tz)?.into_series()))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(!inputs.is_empty() && inputs.len() <= 3, SchemaMismatch: "Expected between 1 and 3 arguments, got {}", inputs.len());
        let data_field = inputs.required((0, "input"))?.to_field(schema)?;
        let format_expr = inputs.required((1, "format"))?;
        let format = format_expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| DaftError::TypeError("format must be a string literal".to_string()))?;

        let timeunit = infer_timeunit_from_format_string(format);

        let timezone = if let Some(tz_expr) = inputs.optional("timezone")? {
            let lit = tz_expr.as_literal();

            if lit == Some(&LiteralValue::Null) {
                None
            } else {
                Some(
                    lit.and_then(|lit| lit.as_str())
                        .ok_or_else(|| {
                            DaftError::TypeError("timezone must be a string literal".to_string())
                        })?
                        .to_string(),
                )
            }
        } else if format_string_has_offset(format) {
            // if it has an offset, we coerce it to UTC. This is consistent with other engines (duckdb, polars)
            Some("UTC".to_string())
        } else {
            None
        };

        Ok(Field::new(
            data_field.name,
            DataType::Timestamp(timeunit, timezone),
        ))
    }
}

#[must_use]
pub fn to_datetime<S: Into<String>>(input: ExprRef, format: S, timezone: Option<S>) -> ExprRef {
    let inputs = if let Some(tz) = timezone {
        vec![input, lit(format.into()), lit(tz.into())]
    } else {
        vec![input, lit(format.into())]
    };
    ScalarFunction::new(ToDatetime, inputs).into()
}

fn to_datetime_impl(
    arr: &Utf8Array,
    format: &str,
    timezone: Option<&str>,
) -> DaftResult<TimestampArray> {
    let len = arr.len();
    let arr_iter = arr.as_arrow().iter();
    let timeunit = infer_timeunit_from_format_string(format);
    let mut timezone = timezone.map(|tz| tz.to_string());
    let arrow_result = arr_iter
            .map(|val| match val {
                Some(val) => {
                    let timestamp = match timezone.as_deref() {
                        Some(tz) => {
                            let datetime = chrono::DateTime::parse_from_str(val, format).map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                ))
                            })?;
                            let datetime_with_timezone = datetime.with_timezone(&tz.parse::<chrono_tz::Tz>().map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Error in to_datetime: failed to parse timezone {tz} : {e}"
                                ))
                            })?);
                            match timeunit {
                                TimeUnit::Seconds => datetime_with_timezone.timestamp(),
                                TimeUnit::Milliseconds => datetime_with_timezone.timestamp_millis(),
                                TimeUnit::Microseconds => datetime_with_timezone.timestamp_micros(),
                                TimeUnit::Nanoseconds => datetime_with_timezone.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                            }
                        }
                        None => {
                            if format_string_has_offset(format) {
                                let datetime = chrono::DateTime::parse_from_str(val, format).map_err(|e| {
                                    DaftError::ComputeError(format!(
                                        "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                    ))
                                })?.to_utc();

                                // if it has an offset, we coerce it to UTC. This is consistent with other engines (duckdb, polars, datafusion)
                                if timezone.is_none() {
                                    timezone = Some("UTC".to_string());
                                }

                                match timeunit {
                                    TimeUnit::Seconds => datetime.timestamp(),
                                    TimeUnit::Milliseconds => datetime.timestamp_millis(),
                                    TimeUnit::Microseconds => datetime.timestamp_micros(),
                                    TimeUnit::Nanoseconds => datetime.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                                }
                            } else {
                                let naive_datetime = chrono::NaiveDateTime::parse_from_str(val, format).map_err(|e| {
                                    DaftError::ComputeError(format!(
                                        "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                    ))
                                })?.and_utc();
                                match timeunit {
                                    TimeUnit::Seconds => naive_datetime.timestamp(),
                                    TimeUnit::Milliseconds => naive_datetime.timestamp_millis(),
                                    TimeUnit::Microseconds => naive_datetime.timestamp_micros(),
                                    TimeUnit::Nanoseconds => naive_datetime.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                                }
                            }
                        }
                    };
                    Ok(Some(timestamp))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<arrow2::array::Int64Array>>()?;

    let result = Int64Array::from((arr.name(), Box::new(arrow_result)));
    let result = TimestampArray::new(
        Field::new(arr.name(), DataType::Timestamp(timeunit, timezone)),
        result,
    );
    assert_eq!(result.len(), len);
    Ok(result)
}
