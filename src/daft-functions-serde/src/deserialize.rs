use daft_dsl::functions::prelude::*;

use crate::format::Format;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Deserialize;

#[derive(FunctionArgs)]
pub struct DeserializeArgs<T> {
    input: T,
    format: Format,
    dtype: DataType,
}

#[typetag::serde]
impl ScalarUDF for Deserialize {
    fn name(&self) -> &'static str {
        "deserialize"
    }

    fn docstring(&self) -> &'static str {
        "Deserializes the expression (string) using the specified format and data type."
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_field(inputs, schema)
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let DeserializeArgs {
            input,
            format,
            dtype,
        } = inputs.try_into()?;
        format.deserializer()(input.utf8()?, &dtype)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryDeserialize;

#[typetag::serde]
impl ScalarUDF for TryDeserialize {
    fn name(&self) -> &'static str {
        "try_deserialize"
    }

    fn docstring(&self) -> &'static str {
        "Deserializes the expression (string) using the specified format and data type, insert null on parsing failures."
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_field(inputs, schema)
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let DeserializeArgs {
            input,
            format,
            dtype,
        } = inputs.try_into()?;
        format.try_deserializer()(input.utf8()?, &dtype)
    }
}

fn get_field(inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
    // validate argument arity
    let DeserializeArgs {
        input,
        format: _,
        dtype,
    } = inputs.try_into()?;
    // validate input argument type, [try_]deserialize currently only supports string inputs.
    let input = input.to_field(schema)?;
    ensure!(
        input.dtype.is_string(),
        "text argument must be of string type."
    );
    // use name of the single argument as the output field name
    Ok(Field::new(input.name, dtype))
}
