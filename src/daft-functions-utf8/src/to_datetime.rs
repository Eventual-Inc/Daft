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

/// Parses a single value into an epoch offset expressed in `timeunit`.
///
/// The parse is strict, mirroring `to_date`: input that the format does not consume is rejected
/// rather than silently dropped, unlike chrono's `parse_and_remainder`.
///
/// A format with no time-of-day field at all (e.g. `%Y-%m-%d`) resolves to midnight on that date,
/// matching DuckDB `strptime`, Polars and Spark. Whether that fallback applies is decided from the
/// format alone (`has_time`), never from the parsed row, so every value in a column is parsed the
/// same way. A partially specified time (e.g. `%Y-%m-%d %H`) therefore stays an error instead of
/// silently discarding the hour and reporting midnight.
///
/// The instant is then resolved in one of three ways:
/// - the format carries an offset directive: the offset in the input defines the instant, and
///   `timezone` only controls how that instant is displayed;
/// - the format is naive and a `timezone` was requested: the value is a local time in that zone;
/// - otherwise the value is read as UTC.
fn parse_to_timestamp(
    val: &str,
    format: &str,
    has_time: bool,
    has_offset: bool,
    timezone: Option<chrono_tz::Tz>,
    timeunit: TimeUnit,
) -> DaftResult<i64> {
    let fail = |e: chrono::format::ParseError| {
        DaftError::ComputeError(format!(
            "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
        ))
    };

    let mut parsed = Parsed::new();
    // Unlike `parse_and_remainder`, `chrono::format::parse` errors when trailing input remains.
    chrono::format::parse(&mut parsed, val, StrftimeItems::new(format)).map_err(fail)?;

    let naive = if has_time {
        // Resolving against the parsed offset mirrors `Parsed::to_datetime`, which matters only
        // when the value is a Unix timestamp (`%s`) that also carries an offset.
        parsed
            .to_naive_datetime_with_offset(parsed.offset().unwrap_or(0))
            .map_err(fail)?
    } else {
        parsed
            .to_naive_date()
            .map_err(fail)?
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always a valid time")
    };

    if has_offset {
        // The input must actually supply an offset. `%Z` only skips a zone name without yielding
        // one, so it fails here rather than silently assuming some zone.
        let offset = parsed.to_fixed_offset().map_err(fail)?;
        let datetime = offset
            .from_local_datetime(&naive)
            .single()
            .expect("a fixed offset maps every local datetime to exactly one instant");
        timestamp_from_datetime(datetime, timeunit, val)
    } else if let Some(tz) = timezone.filter(|_| parsed.timestamp().is_none()) {
        // A naive value with an explicit timezone is a local time in that zone. `%s` is excluded:
        // it already names an absolute instant and must not be re-read as a local time.
        match tz.from_local_datetime(&naive) {
            LocalResult::Single(datetime) => timestamp_from_datetime(datetime, timeunit, val),
            LocalResult::Ambiguous(_, _) => Err(DaftError::ComputeError(format!(
                "Error in to_datetime: ambiguous local datetime {naive} in timezone {tz} for {val}"
            ))),
            LocalResult::None => Err(DaftError::ComputeError(format!(
                "Error in to_datetime: nonexistent local datetime {naive} in timezone {tz} for {val}"
            ))),
        }
    } else {
        timestamp_from_datetime(naive.and_utc(), timeunit, val)
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
    // Decided from the format alone, and once rather than per row, so an all-null column still
    // reports UTC and matches `get_return_field`.
    let timezone = match timezone {
        Some(tz) => Some(tz.to_string()),
        None if has_offset => Some("UTC".to_string()),
        None => None,
    };
    // Resolved once instead of once per row.
    let tz = timezone
        .as_deref()
        .map(|tz| {
            tz.parse::<chrono_tz::Tz>().map_err(|e| {
                DaftError::ComputeError(format!(
                    "Error in to_datetime: failed to parse timezone {tz} : {e}"
                ))
            })
        })
        .transpose()?;

    let result = arr_iter
        .map(|val| {
            val.map(|val| parse_to_timestamp(val, format, has_time, has_offset, tz, timeunit))
                .transpose()
        })
        .collect::<DaftResult<Int64Array>>()?;

    let result = TimestampArray::new(
        Field::new(arr.name(), DataType::Timestamp(timeunit, timezone)),
        result.rename(arr.name()),
    );
    assert_eq!(result.len(), len);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MICROS_PER_DAY: i64 = 86_400_000_000;
    // 05:30 expressed in microseconds, the offset of Asia/Kolkata.
    const IST_OFFSET_MICROS: i64 = (5 * 3_600 + 30 * 60) * 1_000_000;

    fn utf8(values: Vec<Option<&str>>) -> Utf8Array {
        Utf8Array::from_iter("s", values)
    }

    #[test]
    fn test_date_only_format_resolves_to_midnight() {
        let arr = utf8(vec![Some("1970-01-02"), None]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d", None).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY));
        assert_eq!(result.get(1), None);
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, None)
        );
    }

    #[test]
    fn test_date_only_format_with_offset_coerces_to_utc() {
        // Midnight at +05:30 is 18:30 UTC on the previous day.
        let arr = utf8(vec![Some("1970-01-02 +0530")]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d %z", None).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY - IST_OFFSET_MICROS));
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string()))
        );
    }

    #[test]
    fn test_date_only_format_localizes_into_timezone() {
        // Midnight in Asia/Kolkata is 18:30 UTC on the previous day.
        let arr = utf8(vec![Some("1970-01-02")]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d", Some("Asia/Kolkata")).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY - IST_OFFSET_MICROS));
    }

    #[test]
    fn test_nonexistent_midnight_is_rejected() {
        // Cuba starts DST at 00:00 -> 01:00, so this midnight never happens locally.
        let arr = utf8(vec![Some("2018-03-11")]);
        let err = to_datetime_impl(&arr, "%Y-%m-%d", Some("America/Havana")).unwrap_err();
        assert!(
            err.to_string().contains("nonexistent local datetime"),
            "{err}"
        );
    }

    #[test]
    fn test_ambiguous_midnight_is_rejected() {
        // Cuba ends DST at 01:00 -> 00:00, so this midnight happens twice locally.
        let arr = utf8(vec![Some("2018-11-04")]);
        let err = to_datetime_impl(&arr, "%Y-%m-%d", Some("America/Havana")).unwrap_err();
        assert!(
            err.to_string().contains("ambiguous local datetime"),
            "{err}"
        );
    }

    #[test]
    fn test_naive_datetime_localizes_into_timezone() {
        // A format with no offset directive is a local time in the requested timezone, rather
        // than requiring the value to carry an offset of its own.
        let arr = utf8(vec![Some("1970-01-02 00:00:00")]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d %H:%M:%S", Some("Asia/Kolkata")).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY - IST_OFFSET_MICROS));
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, Some("Asia/Kolkata".to_string()))
        );
    }

    #[test]
    fn test_offset_in_input_wins_over_timezone_argument() {
        // With an offset directive the input defines the instant; the timezone only displays it.
        let arr = utf8(vec![Some("1970-01-02 00:00:00 +0000")]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d %H:%M:%S %z", Some("Asia/Kolkata")).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY));
    }

    #[test]
    fn test_unix_timestamp_is_not_localized() {
        // `%s` already names an absolute instant, so an explicit timezone must not shift it.
        let arr = utf8(vec![Some("86400")]);
        let naive = to_datetime_impl(&arr, "%s", None).unwrap();
        let localized = to_datetime_impl(&arr, "%s", Some("Asia/Kolkata")).unwrap();
        assert_eq!(naive.get(0), Some(MICROS_PER_DAY));
        assert_eq!(localized.get(0), Some(MICROS_PER_DAY));
    }

    #[test]
    fn test_all_null_with_offset_format_still_reports_utc() {
        // Regression guard for the get_return_field / runtime dtype mismatch: the output timezone
        // comes from the format, so it must not depend on a row carrying an offset.
        let arr = utf8(vec![None, None]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d %H:%M:%S %z", None).unwrap();
        assert_eq!(result.get(0), None);
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string()))
        );
    }

    #[test]
    fn test_trailing_input_is_rejected() {
        let arr = utf8(vec![Some("2020-01-01T12:34:56.789")]);
        let err = to_datetime_impl(&arr, "%Y-%m-%dT%H:%M:%S", None).unwrap_err();
        assert!(err.to_string().contains("trailing input"), "{err}");
    }

    #[test]
    fn test_partial_time_is_rejected() {
        // The hour must not be silently dropped in favour of midnight.
        let arr = utf8(vec![Some("2020-01-01 12")]);
        let err = to_datetime_impl(&arr, "%Y-%m-%d %H", None).unwrap_err();
        assert!(err.to_string().contains("not enough"), "{err}");
    }

    #[test]
    fn test_escaped_percent_is_not_an_offset() {
        // `%%z` is a literal "%z" in the input, so the result stays naive.
        let arr = utf8(vec![Some("1970-01-02 %z")]);
        let result = to_datetime_impl(&arr, "%Y-%m-%d %%z", None).unwrap();
        assert_eq!(result.get(0), Some(MICROS_PER_DAY));
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microseconds, None)
        );
    }

    #[test]
    fn test_zone_name_without_offset_is_rejected() {
        // chrono skips `%Z` without deriving an offset from it, so the value is refused rather
        // than silently assumed to be in some particular zone.
        let arr = utf8(vec![Some("2020-01-01 00:00:00 UTC")]);
        assert!(to_datetime_impl(&arr, "%Y-%m-%d %H:%M:%S %Z", None).is_err());
    }

    #[test]
    fn test_bad_timezone_is_rejected_even_when_no_row_parses() {
        let arr = utf8(vec![None]);
        let err = to_datetime_impl(&arr, "%Y-%m-%d", Some("not/a/zone")).unwrap_err();
        assert!(
            err.to_string().contains("failed to parse timezone"),
            "{err}"
        );
    }
}
