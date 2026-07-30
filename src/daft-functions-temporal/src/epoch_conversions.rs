use daft_core::{
    datatypes::TimeUnit,
    prelude::{Int64Array, IntoSeries},
};
use daft_dsl::functions::{UnaryArg, prelude::*};

// --- DateFromUnixDate ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DateFromUnixDate;

#[typetag::serde]
impl ScalarUDF for DateFromUnixDate {
    fn name(&self) -> &'static str {
        "date_from_unix_date"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input.cast(&DataType::Int32)?.cast(&DataType::Date)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_integer(),
            TypeError: "Expected integer input, got {}",
            field.dtype
        );
        Ok(Field::new(field.name, DataType::Date))
    }
}

// --- TimestampSeconds ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TimestampSeconds;

#[typetag::serde]
impl ScalarUDF for TimestampSeconds {
    fn name(&self) -> &'static str {
        "timestamp_seconds"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input
            .cast(&DataType::Int64)?
            .cast(&DataType::Timestamp(TimeUnit::Seconds, None))?
            .cast(&DataType::Timestamp(TimeUnit::Microseconds, None))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_numeric(),
            TypeError: "Expected numeric input, got {}",
            field.dtype
        );
        Ok(Field::new(
            field.name,
            DataType::Timestamp(TimeUnit::Microseconds, None),
        ))
    }
}

// --- TimestampMillis ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TimestampMillis;

#[typetag::serde]
impl ScalarUDF for TimestampMillis {
    fn name(&self) -> &'static str {
        "timestamp_millis"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input
            .cast(&DataType::Int64)?
            .cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))?
            .cast(&DataType::Timestamp(TimeUnit::Microseconds, None))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_numeric(),
            TypeError: "Expected numeric input, got {}",
            field.dtype
        );
        Ok(Field::new(
            field.name,
            DataType::Timestamp(TimeUnit::Microseconds, None),
        ))
    }
}

// --- TimestampMicros ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TimestampMicros;

#[typetag::serde]
impl ScalarUDF for TimestampMicros {
    fn name(&self) -> &'static str {
        "timestamp_micros"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        input
            .cast(&DataType::Int64)?
            .cast(&DataType::Timestamp(TimeUnit::Microseconds, None))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_numeric(),
            TypeError: "Expected numeric input, got {}",
            field.dtype
        );
        Ok(Field::new(
            field.name,
            DataType::Timestamp(TimeUnit::Microseconds, None),
        ))
    }
}

// --- FromUnixtime ---

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FromUnixtime;

#[derive(FunctionArgs)]
struct FromUnixtimeArgs<T> {
    input: T,
    #[arg(optional)]
    format: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for FromUnixtime {
    fn name(&self) -> &'static str {
        "from_unixtime"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let FromUnixtimeArgs { input, format } = inputs.try_into()?;
        let fmt = format.as_deref().unwrap_or("%Y-%m-%d %H:%M:%S");
        let ts = input
            .cast(&DataType::Int64)?
            .cast(&DataType::Timestamp(TimeUnit::Seconds, None))?;
        ts.dt_strftime(Some(fmt))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let FromUnixtimeArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            field.dtype.is_numeric(),
            TypeError: "Expected numeric input, got {}",
            field.dtype
        );
        Ok(Field::new(field.name, DataType::Utf8))
    }
}

// --- UnixSeconds / UnixMillis / UnixMicros ---

macro_rules! impl_unix_epoch_fn {
    ($name:ident, $fn_name:literal, $time_unit:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $name;

        #[typetag::serde]
        impl ScalarUDF for $name {
            fn name(&self) -> &'static str {
                $fn_name
            }

            fn call(
                &self,
                inputs: FunctionArgs<Series>,
                _ctx: &daft_dsl::functions::scalar::EvalContext,
            ) -> DaftResult<Series> {
                let UnaryArg { input } = inputs.try_into()?;
                let DataType::Timestamp(input_unit, _) = input.data_type().clone() else {
                    return Err(common_error::DaftError::TypeError(format!(
                        "Expected timestamp input to {}, got {}",
                        $fn_name,
                        input.data_type()
                    )));
                };
                // Timestamps are physically stored as i64 epoch values in `input_unit`
                // (already UTC-based for timezone-aware inputs). Convert between units
                // with floor semantics to match Spark's floorDiv behavior for pre-epoch
                // values, rather than the truncate-toward-zero semantics of a unit cast.
                let physical = input.cast(&DataType::Int64)?;
                let src = input_unit.to_scale_factor();
                let dst = TimeUnit::$time_unit.to_scale_factor();
                if src >= dst {
                    // Floor division: subtract the non-negative remainder first, then
                    // divide exactly. (`Series::floor_div` truncates toward zero for
                    // integer types, which would round pre-epoch values the wrong way.)
                    let factor =
                        Int64Array::from_values("factor", std::iter::once(src / dst)).into_series();
                    let rem = (&physical % &factor)?;
                    let rem = (&rem + &factor)?;
                    let rem = (&rem % &factor)?;
                    let adjusted = (&physical - &rem)?;
                    adjusted.floor_div(&factor)
                } else {
                    let factor =
                        Int64Array::from_values("factor", std::iter::once(dst / src)).into_series();
                    &physical * &factor
                }
            }

            fn get_return_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                let UnaryArg { input } = inputs.try_into()?;
                let field = input.to_field(schema)?;
                ensure!(
                    matches!(field.dtype, DataType::Timestamp(..)),
                    TypeError: "Expected timestamp input to {}, got {}",
                    $fn_name,
                    field.dtype
                );
                Ok(Field::new(field.name, DataType::Int64))
            }
        }
    };
}

impl_unix_epoch_fn!(UnixSeconds, "unix_seconds", Seconds);
impl_unix_epoch_fn!(UnixMillis, "unix_millis", Milliseconds);
impl_unix_epoch_fn!(UnixMicros, "unix_micros", Microseconds);
