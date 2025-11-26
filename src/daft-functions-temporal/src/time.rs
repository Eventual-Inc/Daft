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

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
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
