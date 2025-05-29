use daft_dsl::functions::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonLoads;

#[derive(FunctionArgs)]
pub struct JsonLoadsArgs<T> {
    input: T,
    dtype: DataType,
}

#[typetag::serde]
impl ScalarUDF for JsonLoads {
    fn name(&self) -> &'static str {
        "json_loads"
    }

    fn docstring(&self) -> &'static str {
        "Loads a JSON string as a Daft value, returning null if parsing fails."
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        // validate argument arity
        let JsonLoadsArgs { input, dtype } = inputs.try_into()?;
        // validate input argument type
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype.is_string(),
            "text argument must be of string type."
        );
        // use name of the single argument as the output field name
        Ok(Field::new(input.name, dtype))
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let JsonLoadsArgs { input, dtype } = inputs.try_into()?;
        json::loads(input.utf8()?, &dtype)
    }
}

// All JSON parsing logic for from_json, consider moving out if this needs to be re-used.
mod json {
    use std::sync::Arc;

    use arrow2::{
        datatypes::ArrowDataType,
        io::json::read::{self, json_deserializer::Value},
    };
    use common_error::DaftResult;
    use daft_core::{
        prelude::{DataType, Field, Utf8Array},
        series::Series,
    };

    /// Loads each `text` input to the `dtype`, inserting null on any parsing failure.
    pub fn loads(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series> {
        // needed for conversions later.
        let field = Field::new(input.name(), dtype.clone());
        // parse each item in the array to a JSON Value, then make a JSON Array.
        let json_items: Vec<Value> = input.into_iter().map(parse_item).collect();
        let json_array = Value::Array(json_items);
        // convert the JSON Array into an arrow2 Array
        let arrow2_field = field.to_arrow()?;
        let arrow2_dtype = ArrowDataType::LargeList(Box::new(arrow2_field));
        let arrow2_array = read::deserialize(&json_array, arrow2_dtype)?;
        // convert the arrow2 Array into a Daft Series.
        Series::from_arrow(Arc::new(field), arrow2_array)
    }

    pub fn parse_item(item: Option<&str>) -> Value {
        item.and_then(|text| read::json_deserializer::parse(text.as_bytes()).ok())
            .unwrap_or(Value::Null)
    }
}
