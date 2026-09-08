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
        // `Series::with_utf8_array` passes a `Null` series straight through, so without this
        // check a `Null` input would plan as `Timestamp` but evaluate to `Null` and trip the
        // dtype-mismatch assert. Matches how `to_date` validates via `binary_utf8_to_field`.
        ensure!(data_field.dtype == DataType::Utf8, TypeError: "input must be of type Utf8, got {}", data_field.dtype);
        let format_expr = inputs.required((1, "format"))?;
        let format = format_expr
            .as_literal()
            .and_then(|lit| lit.as_str())
            .ok_or_else(|| DaftError::TypeError("format must be a string literal".to_string()))?;

        let timeunit = infer_timeunit_from_format_string(format);

        let explicit_timezone = if let Some(tz_expr) = inputs.optional("timezone")? {
            let lit = tz_expr.as_literal();

            if lit == Some(&Literal::Null) {
                None
            } else {
                Some(lit.and_then(|lit| lit.as_str()).ok_or_else(|| {
                    DaftError::TypeError("timezone must be a string literal".to_string())
                })?)
            }
        } else {
            None
        };
        let timezone = resolve_output_timezone(format, explicit_timezone);

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

/// Single source of truth for the output timezone.
///
/// An explicit `timezone` always wins. Otherwise, a format containing an offset
/// directive is coerced to UTC (consistent with duckdb, polars, datafusion).
/// This must stay in sync between planning (`get_return_field`) and execution
/// (`to_datetime_impl`): the output dtype is a function of `format` and
/// `timezone` only, never of the data, so all-null/empty inputs resolve identically.
fn resolve_output_timezone(format: &str, timezone: Option<&str>) -> Option<String> {
    if let Some(tz) = timezone {
        Some(tz.to_string())
    } else if format_string_has_offset(format) {
        Some("UTC".to_string())
    } else {
        None
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
    // Resolve the output timezone up front so the kernel dtype always matches
    // `get_return_field`, even when there are no non-null values to inspect.
    let output_timezone = resolve_output_timezone(format, timezone);
    // Hoisted out of the row loop: this selects the parse path and is loop-invariant.
    // Deriving the path from this directly (rather than from `output_timezone`) keeps it
    // independent of `resolve_output_timezone`'s internal precedence rules.
    let has_offset = format_string_has_offset(format);
    // Parse (and thereby validate) an explicit timezone once, rather than per row.
    //
    // Behaviour change: because this no longer happens lazily inside the loop, an invalid or
    // unsupported timezone string now errors deterministically even for an empty or all-null
    // partition, instead of only for partitions that happened to contain a non-null value.
    // Notably `chrono_tz` cannot parse fixed-offset forms such as "+05:30", so those error
    // everywhere rather than silently producing a `Timestamp[.., +05:30]` array on empty input.
    let parsed_timezone = timezone
        .map(|tz| {
            tz.parse::<chrono_tz::Tz>().map_err(|e| {
                DaftError::ComputeError(format!(
                    "Error in to_datetime: failed to parse timezone {tz} : {e}"
                ))
            })
        })
        .transpose()?;
    let result = arr_iter
            .map(|val| match val {
                Some(val) => {
                    let timestamp = match &parsed_timezone {
                        Some(parsed_tz) => {
                            let (datetime, _) = chrono::DateTime::parse_and_remainder(val, format).map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                ))
                            })?;
                            let datetime_with_timezone = datetime.with_timezone(parsed_tz);
                            match timeunit {
                                TimeUnit::Seconds => datetime_with_timezone.timestamp(),
                                TimeUnit::Milliseconds => datetime_with_timezone.timestamp_millis(),
                                TimeUnit::Microseconds => datetime_with_timezone.timestamp_micros(),
                                TimeUnit::Nanoseconds => datetime_with_timezone.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                            }
                        }
                        None if has_offset => {
                            let datetime = chrono::DateTime::parse_and_remainder(val, format).map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                ))
                            })?.0.to_utc();

                            match timeunit {
                                TimeUnit::Seconds => datetime.timestamp(),
                                TimeUnit::Milliseconds => datetime.timestamp_millis(),
                                TimeUnit::Microseconds => datetime.timestamp_micros(),
                                TimeUnit::Nanoseconds => datetime.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                            }
                        }
                        None => {
                            let naive_datetime = chrono::NaiveDateTime::parse_and_remainder(val, format).map_err(|e| {
                                DaftError::ComputeError(format!(
                                    "Error in to_datetime: failed to parse datetime {val} with format {format} : {e}"
                                ))
                            })?.0.and_utc();
                            match timeunit {
                                TimeUnit::Seconds => naive_datetime.timestamp(),
                                TimeUnit::Milliseconds => naive_datetime.timestamp_millis(),
                                TimeUnit::Microseconds => naive_datetime.timestamp_micros(),
                                TimeUnit::Nanoseconds => naive_datetime.timestamp_nanos_opt().ok_or_else(|| DaftError::ComputeError(format!("Error in to_datetime: failed to get nanoseconds for {val}")))?,
                            }
                        }
                    };
                    Ok(Some(timestamp))
                }
                _ => Ok(None),
            })
            .collect::<DaftResult<Int64Array>>()?;

    let result = TimestampArray::new(
        Field::new(arr.name(), DataType::Timestamp(timeunit, output_timezone)),
        result.rename(arr.name()),
    );
    assert_eq!(result.len(), len);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFSET_FMT: &str = "%Y-%m-%dT%H:%M:%S%z";
    const NAIVE_FMT: &str = "%Y-%m-%d %H:%M:%S";
    // 2020-01-01T01:02:03+0100 == 2020-01-01T00:02:03Z == 1577836923s since the epoch.
    const PARSED_MICROS: i64 = 1_577_836_923_000_000;

    #[test]
    fn test_resolve_output_timezone() {
        // No timezone and no offset directive -> naive.
        assert_eq!(resolve_output_timezone(NAIVE_FMT, None), None);
        // An offset directive with no explicit timezone -> coerced to UTC.
        assert_eq!(
            resolve_output_timezone(OFFSET_FMT, None),
            Some("UTC".to_string())
        );
        assert_eq!(resolve_output_timezone("%+", None), Some("UTC".to_string()));
        // An explicit timezone always wins, offset directive or not.
        assert_eq!(
            resolve_output_timezone(NAIVE_FMT, Some("Asia/Shanghai")),
            Some("Asia/Shanghai".to_string())
        );
        assert_eq!(
            resolve_output_timezone(OFFSET_FMT, Some("Asia/Shanghai")),
            Some("Asia/Shanghai".to_string())
        );
    }

    #[test]
    fn test_to_datetime_impl_dtype_is_independent_of_data() {
        // https://github.com/Eventual-Inc/Daft/issues/7470
        // The array dtype must be identical whether or not a row carries an offset.
        let expected = DataType::Timestamp(TimeUnit::Microseconds, Some("UTC".to_string()));

        let all_null = Utf8Array::from_iter("col", vec![None::<&str>, None]);
        assert_eq!(
            to_datetime_impl(&all_null, OFFSET_FMT, None)
                .unwrap()
                .field()
                .dtype,
            expected
        );

        let empty = Utf8Array::from_iter("col", Vec::<Option<&str>>::new());
        assert_eq!(
            to_datetime_impl(&empty, OFFSET_FMT, None)
                .unwrap()
                .field()
                .dtype,
            expected
        );

        let mixed = Utf8Array::from_iter("col", vec![None, Some("2020-01-01T01:02:03+0100")]);
        let result = to_datetime_impl(&mixed, OFFSET_FMT, None).unwrap();
        assert_eq!(result.field().dtype, expected);
        assert_eq!(result.get(0), None);
        assert_eq!(result.get(1), Some(PARSED_MICROS));

        // A naive format must stay naive even with no data to inspect.
        let naive = to_datetime_impl(&all_null, NAIVE_FMT, None).unwrap();
        assert_eq!(
            naive.field().dtype,
            DataType::Timestamp(TimeUnit::Microseconds, None)
        );
    }

    #[test]
    fn test_to_datetime_impl_explicit_timezone_wins_and_validates_without_data() {
        let all_null = Utf8Array::from_iter("col", vec![None::<&str>]);

        // An explicit timezone wins over the offset directive.
        for format in [NAIVE_FMT, OFFSET_FMT] {
            let result = to_datetime_impl(&all_null, format, Some("Asia/Shanghai")).unwrap();
            assert_eq!(
                result.field().dtype,
                DataType::Timestamp(TimeUnit::Microseconds, Some("Asia/Shanghai".to_string()))
            );
        }

        // The timezone is parsed up front, so an unparseable one errors even with no data.
        assert!(to_datetime_impl(&all_null, NAIVE_FMT, Some("Not/AZone")).is_err());
        // chrono_tz has no fixed-offset support, so these error rather than silently passing.
        assert!(to_datetime_impl(&all_null, NAIVE_FMT, Some("+05:30")).is_err());
    }
}
