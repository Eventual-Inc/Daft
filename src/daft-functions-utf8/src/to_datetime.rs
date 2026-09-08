use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    datatypes::{format_string_has_offset, infer_timeunit_from_format_string},
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

/// Parses a value into a [`chrono::NaiveDateTime`] for a format that carries no UTC offset.
///
/// This is strict, mirroring `to_date`: trailing input the format does not consume is rejected
/// instead of silently dropped (consistent with DuckDB and polars). As a special case, a format
/// with no time-of-day field at all (e.g. `%Y-%m-%d`) resolves to midnight on that date, matching
/// DuckDB / polars / Spark. A partially specified time (e.g. `%Y-%m-%d %H`) still errors rather
/// than silently discarding the fields chrono cannot assemble a time from.
fn parse_naive_datetime(val: &str, format: &str) -> DaftResult<chrono::NaiveDateTime> {
    let fail = |e: chrono::format::ParseError| {
        DaftError::ComputeError(format!(
            "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
        ))
    };

    let mut parsed = chrono::format::Parsed::new();
    // Unlike `parse_and_remainder`, `chrono::format::parse` errors when trailing input remains.
    chrono::format::parse(&mut parsed, val, chrono::format::StrftimeItems::new(format))
        .map_err(&fail)?;

    // True when the value+format carry no time-of-day information whatsoever.
    let date_only = parsed.hour_div_12.is_none()
        && parsed.hour_mod_12.is_none()
        && parsed.minute.is_none()
        && parsed.second.is_none()
        && parsed.nanosecond.is_none()
        && parsed.timestamp.is_none();

    match parsed.to_naive_datetime_with_offset(0) {
        Ok(datetime) => Ok(datetime),
        // `NaiveDateTime` needs time fields; a bare date reports `NotEnough`. Fall back to midnight
        // only when nothing time-like was parsed, so partial times keep their error.
        Err(e) if e.kind() == chrono::format::ParseErrorKind::NotEnough && date_only => {
            let date = parsed.to_naive_date().map_err(&fail)?;
            Ok(date
                .and_hms_opt(0, 0, 0)
                .expect("midnight is always a valid time"))
        }
        Err(e) => Err(fail(e)),
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
    let timezone = timezone.map(|tz| tz.to_string());
    let result = arr_iter
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

                                match timeunit {
                                    TimeUnit::Seconds => datetime.timestamp(),
                                    TimeUnit::Milliseconds => datetime.timestamp_millis(),
                                    TimeUnit::Microseconds => datetime.timestamp_micros(),
                                    TimeUnit::Nanoseconds => datetime.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                                }
                            } else {
                                let naive_datetime = parse_naive_datetime(val, format)?.and_utc();
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
            .collect::<DaftResult<Int64Array>>()?;

    // The output timezone must be decided from the format alone, exactly as `get_return_field`
    // does: an offset directive coerces the result to UTC even when no value carries an offset
    // (e.g. an all-null column). Otherwise the runtime array would be built as `Timestamp[us]`
    // and mismatch the planned `Timestamp[us; UTC]`, panicking at execution.
    let timezone = timezone.or_else(|| format_string_has_offset(format).then(|| "UTC".to_string()));

    let result = TimestampArray::new(
        Field::new(arr.name(), DataType::Timestamp(timeunit, timezone)),
        result.rename(arr.name()),
    );
    assert_eq!(result.len(), len);
    Ok(result)
}
