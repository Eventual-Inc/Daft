use daft_dsl::functions::prelude::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ToString;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    #[arg(optional)]
    format: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for ToString {
    fn name(&self) -> &'static str {
        "strftime"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["to_string"]
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, format } = inputs.try_into()?;
        input.dt_strftime(format.as_deref())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(
            matches!(field.dtype, DataType::Time(_) | DataType::Timestamp(_, _) | DataType::Date),
            TypeError: "Expected input to be one of [time, timestamp, date] got {}",
            field.dtype
        );
        Ok(Field::new(field.name, DataType::Utf8))
    }
}
