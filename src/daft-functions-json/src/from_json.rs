use daft_core::{prelude::Utf8Array, series::IntoSeries};
use daft_dsl::{functions::prelude::*, Expr};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FromJson {
    target: DataType,
}

#[derive(FunctionArgs)]
pub struct FromJsonArgs<T> {
    text: T,
}

#[typetag::serde]
impl ScalarUDF for FromJson {
    fn name(&self) -> &'static str {
        "from_json"
    }

    fn docstring(&self) -> &'static str {
        "Parses a JSON string into a Daft value, returning null if parsing fails."
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        // validate argument arity
        let FromJsonArgs { text } = inputs.try_into()?;
        // validate types still since FunctionArgs isn't typed
        let text = input.to_field(schema)?;
        ensure!(text.dtype.is_string(), "input must be a string type.");
        // use name of the single argument as the output field name
        Ok(Field::new(text.name, self.return_type))
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let text = inputs.required(0)?;

        let arr = text
            .utf8()
            .expect("type should have been validated already during `function_args_to_field`")
            .into_iter()
            .map(|s_opt| s_opt.map(|s| s.to_uppercase()))
            .collect::<Utf8Array>();
        Ok(arr.into_series())
    }
}
