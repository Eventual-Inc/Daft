use chrono::{
    LocalResult, TimeZone,
    format::{Parsed, strftime::StrftimeItems},
};
use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    datatypes::{
        format_string_has_offset, format_string_has_time, infer_timeunit_from_format_string,
    },
    prelude::*,
    series::IntoSeries,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
    lit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToDatetime;

#[typetag::serde]
impl ScalarUDF for ToDatetime {
    fn name(&self) -> &'static str {
        "to_datetime"
    }
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
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

            if lit == Some(&Literal::Null) {
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
    ScalarFn::builtin(ToDatetime, inputs).into()
}

fn timestamp_from_datetime<Tz: TimeZone>(
    datetime: chrono::DateTime<Tz>,
    timeunit: TimeUnit,
    val: &str,
) -> DaftResult<i64> {
    match timeunit {
        TimeUnit::Seconds => Ok(datetime.timestamp()),
        TimeUnit::Milliseconds => Ok(datetime.timestamp_millis()),
        TimeUnit::Microseconds => Ok(datetime.timestamp_micros()),
        TimeUnit::Nanoseconds => datetime.timestamp_nanos_opt().ok_or_else(|| {
            DaftError::ComputeError(format!(
                "Error in to_datetime: failed to get nanoseconds for {val}"
            ))
        }),
    }
}

fn timestamp_from_naive_datetime(
    naive: chrono::NaiveDateTime,
    timeunit: TimeUnit,
    val: &str,
) -> DaftResult<i64> {
    timestamp_from_datetime(naive.and_utc(), timeunit, val)
}

/// Parses a date-only value (a format without time fields) as midnight.
///
/// Uses a strict parse so trailing input is rejected, matching `to_date` and other engines.
/// An offset embedded in the input (e.g. `%z`) is preserved: midnight is resolved with that
/// offset before coercing to UTC or the explicit timezone.
fn parse_date_only_to_timestamp(
    val: &str,
    format: &str,
    timezone: Option<&str>,
    timeunit: TimeUnit,
) -> DaftResult<i64> {
    let mut parsed = Parsed::new();
    chrono::format::parse(&mut parsed, val, StrftimeItems::new(format)).map_err(|e| {
        DaftError::ComputeError(format!(
            "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
        ))
    })?;
    let date = parsed.to_naive_date().map_err(|e| {
        DaftError::ComputeError(format!(
            "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
        ))
    })?;
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always a valid time");

    let input_offset = parsed.to_fixed_offset().ok();

    match (input_offset, timezone) {
        (Some(offset), Some(tz_str)) => {
            let tz = tz_str.parse::<chrono_tz::Tz>().map_err(|e| {
                DaftError::ComputeError(format!(
                    "Error in to_datetime: failed to parse timezone {tz_str} : {e}"
                ))
            })?;
            let dt_fixed = offset.from_local_datetime(&midnight).single().ok_or_else(|| {
                DaftError::ComputeError(format!(
                    "Error in to_datetime: failed to resolve midnight {midnight} with offset {offset} for {val}"
                ))
            })?;
            timestamp_from_datetime(dt_fixed.with_timezone(&tz), timeunit, val)
        }
        (Some(offset), None) => {
            let dt_utc = offset
                .from_local_datetime(&midnight)
                .single()
                .ok_or_else(|| {
                    DaftError::ComputeError(format!(
                        "Error in to_datetime: failed to resolve midnight {midnight} with offset {offset} for {val}"
                    ))
                })?
                .with_timezone(&chrono::Utc);
            timestamp_from_datetime(dt_utc, timeunit, val)
        }
        (None, Some(tz_str)) => {
            let tz = tz_str.parse::<chrono_tz::Tz>().map_err(|e| {
                DaftError::ComputeError(format!(
                    "Error in to_datetime: failed to parse timezone {tz_str} : {e}"
                ))
            })?;
            match tz.from_local_datetime(&midnight) {
                LocalResult::Single(dt) => timestamp_from_datetime(dt, timeunit, val),
                LocalResult::Ambiguous(_, _) => Err(DaftError::ComputeError(format!(
                    "Error in to_datetime: ambiguous local datetime {midnight} in timezone {tz_str} for {val}"
                ))),
                LocalResult::None => Err(DaftError::ComputeError(format!(
                    "Error in to_datetime: nonexistent local datetime {midnight} in timezone {tz_str} for {val}"
                ))),
            }
        }
        (None, None) => timestamp_from_naive_datetime(midnight, timeunit, val),
    }
}

fn to_datetime_impl(
    arr: &Utf8Array,
    format: &str,
    timezone: Option<&str>,
) -> DaftResult<TimestampArray> {
    let len = arr.len();
    let arr_iter = arr.into_iter();
    let timeunit = infer_timeunit_from_format_string(format);
    let has_time = format_string_has_time(format);
    let has_offset = format_string_has_offset(format);
    // If the input carries an offset, we coerce it to UTC. This is consistent with other engines (duckdb, polars, datafusion).
    // Resolved once so all-null inputs still report UTC, matching `get_return_field`.
    let timezone = match timezone {
        Some(tz) => Some(tz.to_string()),
        None if has_offset => Some("UTC".to_string()),
        None => None,
    };
    let result = arr_iter
            .map(|val| match val {
                Some(val) => {
                    let timestamp = match timezone.as_deref() {
                        Some(tz) => {
                            if has_time {
                                // Strict parse: trailing input is an error (unlike `parse_and_remainder`).
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
                                timestamp_from_datetime(datetime_with_timezone, timeunit, val)?
                            } else {
                                // A format without time fields resolves to midnight,
                                // matching DuckDB `strptime`, Polars and Spark.
                                parse_date_only_to_timestamp(val, format, Some(tz), timeunit)?
                            }
                        }
                        None => {
                            if has_time {
                                // Strict parse: trailing input is an error (unlike `parse_and_remainder`).
                                let naive_datetime = chrono::NaiveDateTime::parse_from_str(val, format).map_err(|e| {
                                    DaftError::ComputeError(format!(
                                        "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                    ))
                                })?;
                                timestamp_from_naive_datetime(naive_datetime, timeunit, val)?
                            } else {
                                // A format without time fields resolves to midnight,
                                // matching DuckDB `strptime`, Polars and Spark.
                                parse_date_only_to_timestamp(val, format, None, timeunit)?
                            }
                        }
                    };
                    Ok(Some(timestamp))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<Int64Array>>()?;

    let result = TimestampArray::new(
        Field::new(arr.name(), DataType::Timestamp(timeunit, timezone)),
        result.rename(arr.name()),
    );
    assert_eq!(result.len(), len);
    Ok(result)
}
