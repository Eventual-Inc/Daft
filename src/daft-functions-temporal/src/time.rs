use std::sync::Arc;

use arrow_array::Int64Array;
use common_error::DaftError;
use daft_core::prelude::{AsArrow, TimeUnit};
use daft_dsl::functions::{UnaryArg, prelude::*};
use daft_schema::time_unit::{
    naive_datetime_to_timestamp, naive_local_to_timestamp, parse_timezone,
    timestamp_to_naive_datetime, timestamp_to_naive_local,
};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Time;

#[typetag::serde]
impl ScalarUDF for Time {
    fn name(&self) -> &'static str {
        "time"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input.dt_time()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        match field.dtype {
            DataType::Time(_) => Ok(field),
            DataType::Timestamp(tu, _) => {
                let tu = match tu {
                    TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                    _ => TimeUnit::Microseconds,
                };
                Ok(Field::new(field.name, DataType::Time(tu)))
            }
            _ => Err(DaftError::TypeError(format!(
                "Expected input to time to be time or timestamp, got {}",
                field.dtype
            ))),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConvertTimeZone;

#[derive(FunctionArgs)]
struct ConvertArgs<T> {
    input: T,
    to_timezone: String,
    #[arg(optional)]
    from_timezone: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for ConvertTimeZone {
    fn name(&self) -> &'static str {
        "convert_time_zone"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ConvertArgs {
            input,
            to_timezone,
            from_timezone,
        } = inputs.try_into()?;
        input.dt_convert_time_zone(&to_timezone, from_timezone.as_deref())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ConvertArgs {
            input,
            to_timezone,
            from_timezone,
        } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let DataType::Timestamp(timeunit, tz) = &field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a timestamp, got {}",
                field.dtype
            )));
        };

        ensure!(
            !(tz.is_none() && from_timezone.is_none()),
            ValueError: "from_timezone must be provided for timestamps without a timezone"
        );

        Ok(Field::new(
            field.name,
            DataType::Timestamp(*timeunit, Some(to_timezone)),
        ))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ReplaceTimeZone;

#[derive(FunctionArgs)]
struct ReplaceArgs<T> {
    input: T,
    #[arg(optional)]
    timezone: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for ReplaceTimeZone {
    fn name(&self) -> &'static str {
        "replace_time_zone"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ReplaceArgs { input, timezone } = inputs.try_into()?;
        input.dt_replace_time_zone(timezone.as_deref())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ReplaceArgs { input, timezone } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let DataType::Timestamp(timeunit, _) = &field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected input to be a timestamp, got {}",
                field.dtype
            )));
        };

        Ok(Field::new(
            field.name,
            DataType::Timestamp(*timeunit, timezone),
        ))
    }
}

// --- FromUtcTimestamp ---
//
// Spark semantics: interprets the input as a UTC instant (regardless of any tz
// label) and returns a tz-naive timestamp representing the wall-clock time in
// the given timezone. The output i64 encodes that wall-clock as if it were
// UTC, so a downstream reader formatting the result without a tz sees the
// shifted local time.

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FromUtcTimestamp;

#[derive(FunctionArgs)]
struct UtcConversionArgs<T> {
    input: T,
    timezone: String,
}

#[typetag::serde]
impl ScalarUDF for FromUtcTimestamp {
    fn name(&self) -> &'static str {
        "from_utc_timestamp"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UtcConversionArgs { input, timezone } = inputs.try_into()?;
        let tz_parsed = parse_timezone(&timezone)?;

        let DataType::Timestamp(time_unit, _) = input.data_type().clone() else {
            return Err(DaftError::TypeError(format!(
                "Expected timestamp input to from_utc_timestamp, got {}",
                input.data_type()
            )));
        };

        let ts_array = input.timestamp()?;
        let physical = ts_array.as_arrow()?;

        let mut values: Vec<Option<i64>> = Vec::with_capacity(physical.len());
        for opt in physical {
            match opt {
                None => values.push(None),
                Some(ts) => {
                    let naive_local = timestamp_to_naive_local(ts, time_unit, &tz_parsed);
                    values.push(Some(naive_datetime_to_timestamp(naive_local, time_unit)?));
                }
            }
        }

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(
                input.name().to_string(),
                DataType::Timestamp(time_unit, None),
            )),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UtcConversionArgs { input, timezone } = inputs.try_into()?;
        parse_timezone(&timezone)?;
        let field = input.to_field(schema)?;
        let DataType::Timestamp(timeunit, _) = &field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected timestamp input to from_utc_timestamp, got {}",
                field.dtype
            )));
        };
        Ok(Field::new(field.name, DataType::Timestamp(*timeunit, None)))
    }
}

// --- ToUtcTimestamp ---
//
// Spark semantics: interprets the input wall-clock as being in the given
// timezone and returns the equivalent UTC instant as a tz-naive timestamp.
// For tz-aware inputs the wall-clock is first extracted via the input's own
// timezone label, then re-interpreted in the target tz (matches Spark's
// behavior of always treating the displayed wall-clock as the input).

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToUtcTimestamp;

#[typetag::serde]
impl ScalarUDF for ToUtcTimestamp {
    fn name(&self) -> &'static str {
        "to_utc_timestamp"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UtcConversionArgs { input, timezone } = inputs.try_into()?;
        let tz_parsed = parse_timezone(&timezone)?;

        let DataType::Timestamp(time_unit, input_tz) = input.data_type().clone() else {
            return Err(DaftError::TypeError(format!(
                "Expected timestamp input to to_utc_timestamp, got {}",
                input.data_type()
            )));
        };

        let input_tz_parsed = input_tz.as_deref().map(parse_timezone).transpose()?;

        let ts_array = input.timestamp()?;
        let physical = ts_array.as_arrow()?;

        let mut values: Vec<Option<i64>> = Vec::with_capacity(physical.len());
        for opt in physical {
            match opt {
                None => values.push(None),
                Some(ts) => {
                    let naive_local = match &input_tz_parsed {
                        Some(in_tz) => timestamp_to_naive_local(ts, time_unit, in_tz),
                        None => timestamp_to_naive_datetime(ts, time_unit),
                    };
                    let utc_ts =
                        naive_local_to_timestamp(naive_local, time_unit, &tz_parsed, &timezone)?;
                    values.push(Some(utc_ts));
                }
            }
        }

        let arrow_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(values));
        Series::from_arrow(
            Arc::new(Field::new(
                input.name().to_string(),
                DataType::Timestamp(time_unit, None),
            )),
            arrow_arr,
        )
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UtcConversionArgs { input, timezone } = inputs.try_into()?;
        parse_timezone(&timezone)?;
        let field = input.to_field(schema)?;
        let DataType::Timestamp(timeunit, _) = &field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected timestamp input to to_utc_timestamp, got {}",
                field.dtype
            )));
        };
        Ok(Field::new(field.name, DataType::Timestamp(*timeunit, None)))
    }
}
