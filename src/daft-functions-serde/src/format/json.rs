use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_arrow::{
    array::Utf8Array as ArrowUtf8Array,
    datatypes::DataType as ArrowDataType,
    io::json::{
        read::{self, json_deserializer::Value},
        write::new_serializer,
    },
    offset::Offsets,
};
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
    // convert the JSON Array into an arrow2 Array
    #[allow(deprecated, reason = "arrow2 migration")]
    let arrow2_field = field.to_arrow2()?;
    let arrow2_dtype = ArrowDataType::LargeList(Box::new(arrow2_field));
    let arrow2_array = read::deserialize(&json_array, arrow2_dtype)?;
    // convert the arrow2 Array into a Daft Series.
    Series::from_arrow(Arc::new(field), arrow2_array)
}

// Parses a single item.
pub fn parse_item(item: Option<&str>) -> DaftResult<Value<'_>> {
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
    // convert the JSON Array into an arrow2 Array
    #[allow(deprecated, reason = "arrow2 migration")]
    let arrow2_field = field.to_arrow2()?;
    let arrow2_dtype = ArrowDataType::LargeList(Box::new(arrow2_field));
    let arrow2_array = read::deserialize(&json_array, arrow2_dtype)?;
    // convert the arrow2 Array into a Daft Series.
    Series::from_arrow(Arc::new(field), arrow2_array)
}

/// Parses a single item, inserting null on any parsing failure.
pub fn try_parse_item(item: Option<&str>) -> Value<'_> {
    item.and_then(|text| read::json_deserializer::parse(text.as_bytes()).ok())
        .unwrap_or(Value::Null)
}

/// Serializes each input value as a JSON string.
pub fn serialize(input: Series) -> DaftResult<Utf8Array> {
    // setup inputs
    let name = input.name();
    #[allow(deprecated, reason = "arrow2 migration")]
    let input = input.to_arrow2();
    let validity = input.validity().cloned();
    // setup outputs
    let mut values = Vec::<u8>::new();
    let mut offsets = Offsets::<i64>::new();
    let mut serializer = new_serializer(input.as_ref(), 0, usize::MAX);
    // drive the serializer
    while let Some(bytes) = serializer.next() {
        offsets.try_push(bytes.len() as i64)?;
        values.extend(bytes);
    }
    // create the daft array
    let array = ArrowUtf8Array::new(
        ArrowDataType::LargeUtf8,
        offsets.into(),
        values.into(),
        validity,
    );
    let array = Box::new(array);
    Ok(Utf8Array::from((name, array)))
}

/// Serializes each input value as a JSON string, inserting null on any failures.
pub fn try_serialize(_: Series) -> DaftResult<Utf8Array> {
    // try_serialize will require deeper arrow2 json work, and it is not immediately obvious if it's even useful, so punting here.
    Err(DaftError::ComputeError(
        "try_serialize with json is not currently supported.".to_string(),
    ))
}
