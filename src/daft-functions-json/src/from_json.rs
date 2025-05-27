use daft_core::{prelude::Utf8Array, series::IntoSeries};
use daft_dsl::{functions::prelude::*, Expr};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FromJson;

#[derive(FunctionArgs)]
pub struct FromJsonArgs<T> {
    input: T,
    dtype: T,
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
        let FromJsonArgs { input, dtype } = inputs.try_into()?;

        // validate types still since FunctionArgs isn't typed
        let input_field = input.to_field(schema)?;
        let dtype_field = dtype.to_field(schema)?;
        ensure!(
            input_field.dtype.is_string(),
            "input must be a string type."
        );
        ensure!(
            dtype_field.dtype.is_string(),
            "dtype must be a string type."
        );

        // parse the return type argument to determine this return type.
        let name = input_field.name;
        let return_type = Self::try_parse_return_type(dtype)?;
        Ok(Field::new(name, return_type))
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

impl FromJson {
    // Need to parse argument literal to determine the output type.
    fn try_parse_return_type(arg: ExprRef) -> DaftResult<DataType> {
        if let Expr::Literal(lit) = arg.as_ref() {
            if let Some(lit) = lit.as_str() {
                DataType::try_from_sql(lit)
            } else {
                value_err!("from_json schema must be a string literal.")
            }
        } else {
            value_err!("from_json schema must be a string literal.")
        }
    }
}
