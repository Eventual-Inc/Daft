use daft_core::datatypes::TimeUnit;
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
