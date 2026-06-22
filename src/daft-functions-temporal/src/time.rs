use common_error::DaftError;
use daft_core::prelude::TimeUnit;
use daft_dsl::functions::{UnaryArg, prelude::*};

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
