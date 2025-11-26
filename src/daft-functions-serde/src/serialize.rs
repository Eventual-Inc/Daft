use daft_core::series::IntoSeries;
use daft_dsl::functions::prelude::*;

use crate::format::Format;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Serialize;

#[derive(FunctionArgs)]
pub struct SerializeArgs<T> {
    input: T,
    format: Format,
}

#[typetag::serde]
impl ScalarUDF for Serialize {
    fn name(&self) -> &'static str {
        "serialize"
    }

    fn docstring(&self) -> &'static str {
        "Serializes the expression as a string using the specified format."
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_field(inputs, schema)
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let SerializeArgs { input, format } = inputs.try_into()?;
        format.serializer()(input).map(|array| array.into_series())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TrySerialize;

#[typetag::serde]
impl ScalarUDF for TrySerialize {
    fn name(&self) -> &'static str {
        "try_serialize"
    }

    fn docstring(&self) -> &'static str {
        "Serializes the expression as a string using the specified format, insert null on failures."
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        get_field(inputs, schema)
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let SerializeArgs { input, format } = inputs.try_into()?;
        format.try_serializer()(input).map(|array| array.into_series())
    }
}

fn get_field(inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
    // validate argument arity
    let SerializeArgs { input, format: _ } = inputs.try_into()?;
    // [try_]serialize supports any arbitrary value
    let input = input.to_field(schema)?;
    // use name of the single argument as the output field name
    Ok(Field::new(input.name, DataType::Utf8))
}
