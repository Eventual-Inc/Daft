use daft_core::prelude::TimeUnit;
use daft_dsl::functions::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UnixTimestamp;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    time_unit: TimeUnit,
}

#[typetag::serde]
impl ScalarUDF for UnixTimestamp {
    fn name(&self) -> &'static str {
        "to_unix_epoch"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, time_unit } = inputs.try_into()?;
        input
            .cast(&DataType::Timestamp(time_unit, None))
            .and_then(|s| s.cast(&DataType::Int64))
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        ensure!(
            matches!(field.dtype, DataType::Timestamp(..) | DataType::Date),
            TypeError: "Expected input to be date or timestamp, got {}",
            field.dtype
        );

        Ok(Field::new(field.name, DataType::Int64))
    }
}
