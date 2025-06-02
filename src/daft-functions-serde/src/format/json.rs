use std::sync::Arc;

use arrow2::{
    datatypes::ArrowDataType,
    io::json::read::{self, json_deserializer::Value},
};
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Utf8Array},
    series::Series,
};

/// Deserializes each `text` input to the `dtype`.
pub(crate) fn deserialize(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series> {
    let field = Field::new(input.name(), dtype.clone());
    // parse each item in the array to a JSON Value, then make a JSON Array.
    let json_items: Vec<Value> = input
        .into_iter()
        .map(parse_item)
        .collect::<DaftResult<Vec<_>>>()?;
    let json_array = Value::Array(json_items);
    dbg!(&json_array);
    // convert the JSON Array into an arrow2 Array
    let arrow2_field = field.to_arrow()?;
    let arrow2_dtype = ArrowDataType::LargeList(Box::new(arrow2_field));
    let arrow2_array = read::deserialize(&json_array, arrow2_dtype)?;
    dbg!(&arrow2_array);
    // convert the arrow2 Array into a Daft Series.
    Series::from_arrow(Arc::new(field), arrow2_array)
}

// Parses a single item.
pub fn parse_item(item: Option<&str>) -> DaftResult<Value> {
    item.map_or(Ok(Value::Null), |text| {
        read::json_deserializer::parse(text.as_bytes())
            .map_err(|e| DaftError::ValueError(format!("Failed to parse JSON: {}", e)))
    })
}

/// Deserializes each `text` input to the `dtype`, inserting null on any parsing failure.
pub fn try_deserialize(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series> {
    let field = Field::new(input.name(), dtype.clone());
    // parse each item in the array to a JSON Value, then make a JSON Array.
    let json_items: Vec<Value> = input.into_iter().map(try_parse_item).collect();
    let json_array = Value::Array(json_items);
    dbg!(&json_array);
    // convert the JSON Array into an arrow2 Array
    let arrow2_field = field.to_arrow()?;
    let arrow2_dtype = ArrowDataType::LargeList(Box::new(arrow2_field));
    let arrow2_array = read::deserialize(&json_array, arrow2_dtype)?;
    dbg!(&arrow2_array);
    // convert the arrow2 Array into a Daft Series.
    Series::from_arrow(Arc::new(field), arrow2_array)
}

/// Parses a single item, inserting null on any parsing failure.
pub fn try_parse_item(item: Option<&str>) -> Value {
    item.and_then(|text| read::json_deserializer::parse(text.as_bytes()).ok())
        .unwrap_or(Value::Null)
}
