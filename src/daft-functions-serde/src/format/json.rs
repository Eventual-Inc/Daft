use std::sync::Arc;

use arrow::array::{LargeStringArray, OffsetBufferBuilder};
use arrow_json::{EncoderOptions, ReaderBuilder, writer::make_encoder};
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, FullNull, Utf8Array},
    series::Series,
};

/// Deserializes each `text` input to the `dtype`.
pub(crate) fn deserialize(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series> {
    if input.is_empty() {
        return Ok(Series::empty(input.name(), dtype));
    }
    let field = Field::new(input.name(), dtype.clone());

    let arrow_field = Arc::new(field.to_arrow()?);

    let mut decoder = ReaderBuilder::new_with_field(arrow_field)
        .with_coerce_primitive(true)
        .with_batch_size(input.len())
        .build_decoder()?;

    // fast path for no nulls
    if let Ok(values) = input.values() {
        for value in values {
            decoder.decode(value.as_bytes())?;
            decoder.decode(b" ")?;
        }
    } else {
        for value in input {
            let value = value.map(|v| v.as_bytes()).unwrap_or_else(|| b"null");
            decoder.decode(value)?;
            decoder.decode(b" ")?;
        }
    }

    let rb = decoder.flush()?;
    let rb = rb.ok_or_else(|| {
        DaftError::ValueError("expected non empty recordbatch during deserialization".to_string())
    })?;

    let col = rb.column(0);

    // convert the arrow2 Array into a Daft Series.
    Series::from_arrow(field, col.clone())
}

/// Deserializes each `text` input to the `dtype`, inserting null on any parsing failure.
pub fn try_deserialize(input: &Utf8Array, dtype: &DataType) -> DaftResult<Series> {
    if input.is_empty() {
        return Ok(Series::empty(input.name(), dtype));
    }
    let field = Field::new(input.name(), dtype.clone());

    let arrow_field = Arc::new(field.to_arrow()?);

    // We can't use decoder.decode() here because a single malformed value
    // corrupts the tape decoder state. Instead, parse each value to
    // serde_json::Value (with fallback to Null) and use decoder.serialize().
    let values: Vec<serde_json::Value> = input
        .into_iter()
        .map(|item| {
            item.and_then(|text| serde_json::from_str(text).ok())
                .unwrap_or(serde_json::Value::Null)
        })
        .collect();

    let mut decoder = ReaderBuilder::new_with_field(arrow_field)
        .with_coerce_primitive(true)
        .with_batch_size(values.len())
        .build_decoder()?;

    decoder.serialize(&values)?;

    let rb = decoder.flush()?;
    let rb = rb.ok_or_else(|| {
        DaftError::ValueError("expected non empty recordbatch during deserialization".to_string())
    })?;

    let col = rb.column(0);
    Series::from_arrow(field, col.clone())
}

/// Serializes each input value as a JSON string.
pub fn serialize(input: Series) -> DaftResult<Utf8Array> {
    let name = input.name();
    if input.is_empty() {
        return Ok(Utf8Array::empty(name, &DataType::Utf8));
    }
    let field = Field::new(name, input.data_type().clone());
    let nulls = input.nulls().cloned();
    let arrow_array = input.to_arrow()?;
    let arrow_field = Arc::new(field.to_arrow()?);

    let options = EncoderOptions::default().with_explicit_nulls(true);
    let mut encoder = make_encoder(&arrow_field, arrow_array.as_ref(), &options)?;

    let mut values = Vec::<u8>::new();
    let mut offsets = OffsetBufferBuilder::new(arrow_array.len());

    for idx in 0..arrow_array.len() {
        let start = values.len();
        encoder.encode(idx, &mut values);
        offsets.push_length(values.len() - start);
    }

    let array = LargeStringArray::new(offsets.finish(), values.into(), nulls);
    Utf8Array::from_arrow(Field::new(name, DataType::Utf8), Arc::new(array))
}

/// Serializes each input value as a JSON string, inserting null on any failures.
pub fn try_serialize(_: Series) -> DaftResult<Utf8Array> {
    // try_serialize will require deeper arrow2 json work, and it is not immediately obvious if it's even useful, so punting here.
    Err(DaftError::ComputeError(
        "try_serialize with json is not currently supported.".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, FullNull};

    use super::*;

    #[test]
    fn test_deserialize_int64() {
        let input = Utf8Array::from_slice("col", &["1", "2", "3"]);
        let result = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.data_type(), &DataType::Int64);
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn test_deserialize_float64() {
        let input = Utf8Array::from_slice("col", &["1.5", "2.7", "3.0"]);
        let result = deserialize(&input, &DataType::Float64).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.f64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(1.5), Some(2.7), Some(3.0)]);
    }

    #[test]
    fn test_deserialize_boolean() {
        let input = Utf8Array::from_slice("col", &["true", "false", "true"]);
        let result = deserialize(&input, &DataType::Boolean).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.bool().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(true), Some(false), Some(true)]);
    }

    #[test]
    fn test_deserialize_utf8() {
        let input = Utf8Array::from_slice("col", &[r#""hello""#, r#""world""#]);
        let result = deserialize(&input, &DataType::Utf8).unwrap();
        assert_eq!(result.len(), 2);
        let arr = result.utf8().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some("hello"), Some("world")]);
    }

    #[test]
    fn test_deserialize_null_inputs() {
        let input = Utf8Array::from_iter("col", [Some("1"), None, Some("3")]);
        let result = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_deserialize_json_null_literal() {
        let input = Utf8Array::from_slice("col", &["null", "42", "null"]);
        let result = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![None, Some(42), None]);
    }

    #[test]
    fn test_deserialize_empty_input() {
        let input = Utf8Array::from_iter("col", std::iter::empty::<Option<&str>>());
        let result = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_deserialize_struct() {
        let input =
            Utf8Array::from_slice("col", &[r#"{"a": 1, "b": "x"}"#, r#"{"a": 2, "b": "y"}"#]);
        let dtype = DataType::Struct(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let result = deserialize(&input, &dtype).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.data_type(), &dtype);
    }

    #[test]
    fn test_deserialize_list() {
        let input = Utf8Array::from_slice("col", &["[1, 2]", "[3, 4, 5]"]);
        let dtype = DataType::List(Box::new(DataType::Int64));
        let result = deserialize(&input, &dtype).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.data_type(), &dtype);
    }

    #[test]
    fn test_deserialize_invalid_json_errors() {
        let input = Utf8Array::from_slice("col", &["not valid json"]);
        let result = deserialize(&input, &DataType::Int64);
        dbg!(&result);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_preserves_name() {
        let input = Utf8Array::from_slice("my_column", &["42"]);
        let result = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.name(), "my_column");
    }

    // -- try_deserialize tests --

    #[test]
    fn test_try_deserialize_valid() {
        let input = Utf8Array::from_slice("col", &["1", "2", "3"]);
        let result = try_deserialize(&input, &DataType::Int64).unwrap();
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(1), Some(2), Some(3)]);
    }

    #[test]
    fn test_try_deserialize_invalid_becomes_null() {
        let input = Utf8Array::from_slice("col", &["1", "not json", "3"]);
        let result = try_deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 3);
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_try_deserialize_null_inputs() {
        let input = Utf8Array::from_iter("col", [Some("10"), None, Some("30")]);
        let result = try_deserialize(&input, &DataType::Int64).unwrap();
        let arr = result.i64().unwrap();
        let vals: Vec<_> = arr.into_iter().collect();
        assert_eq!(vals, vec![Some(10), None, Some(30)]);
    }

    #[test]
    fn test_try_deserialize_all_invalid() {
        let input = Utf8Array::from_slice("col", &["bad", "also bad", "{broken"]);
        let result = try_deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.null_count(), 3);
    }

    // -- serialize round-trip tests --

    #[test]
    fn test_serialize_int64() {
        let input = Utf8Array::from_slice("col", &["1", "2", "3"]);
        let series = deserialize(&input, &DataType::Int64).unwrap();
        let output = serialize(series).unwrap();
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(vals, vec![Some("1"), Some("2"), Some("3")]);
    }

    #[test]
    fn test_serialize_boolean() {
        let input = Utf8Array::from_slice("col", &["true", "false"]);
        let series = deserialize(&input, &DataType::Boolean).unwrap();
        let output = serialize(series).unwrap();
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(vals, vec![Some("true"), Some("false")]);
    }

    #[test]
    fn test_roundtrip_utf8() {
        let input = Utf8Array::from_slice("col", &[r#""hello""#, r#""world""#]);
        let series = deserialize(&input, &DataType::Utf8).unwrap();
        let output = serialize(series).unwrap();
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(vals, vec![Some(r#""hello""#), Some(r#""world""#)]);
    }

    #[test]
    fn test_roundtrip_struct() {
        let input_str = r#"{"a":1,"b":"x"}"#;
        let input = Utf8Array::from_slice("col", &[input_str]);
        let dtype = DataType::Struct(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let series = deserialize(&input, &dtype).unwrap();
        let output = serialize(series).unwrap();
        assert_eq!(output.len(), 1);
        // Verify it round-trips to valid JSON that re-deserializes
        let round2 = deserialize(&output, &dtype).unwrap();
        assert_eq!(round2.len(), 1);
    }

    #[test]
    fn test_serialize_float64() {
        let input = Utf8Array::from_slice("col", &["1.5", "2.7", "3.0"]);
        let series = deserialize(&input, &DataType::Float64).unwrap();
        let output = serialize(series).unwrap();
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(vals, vec![Some("1.5"), Some("2.7"), Some("3.0")]);
    }

    #[test]
    fn test_serialize_with_nulls() {
        let input = Utf8Array::from_iter("col", [Some("1"), None, Some("3")]);
        let series = deserialize(&input, &DataType::Int64).unwrap();
        let output = serialize(series).unwrap();
        assert_eq!(output.len(), 3);
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(vals, vec![Some("1"), None, Some("3")]);
    }

    #[test]
    fn test_serialize_preserves_name() {
        let input = Utf8Array::from_slice("my_col", &["42"]);
        let series = deserialize(&input, &DataType::Int64).unwrap();
        let output = serialize(series).unwrap();
        assert_eq!(output.name(), "my_col");
    }

    #[test]
    fn test_serialize_list() {
        let input = Utf8Array::from_slice("col", &["[1,2]", "[3,4,5]"]);
        let dtype = DataType::List(Box::new(DataType::Int64));
        let series = deserialize(&input, &dtype).unwrap();
        let output = serialize(series).unwrap();
        assert_eq!(output.len(), 2);
        // Round-trip: re-deserialize the serialized output
        let round2 = deserialize(&output, &dtype).unwrap();
        assert_eq!(round2.len(), 2);
    }

    #[test]
    fn test_roundtrip_nested_struct() {
        let input_str = r#"{"a":{"x":1},"b":[2,3]}"#;
        let inner_struct = DataType::Struct(vec![Field::new("x", DataType::Int64)]);
        let dtype = DataType::Struct(vec![
            Field::new("a", inner_struct),
            Field::new("b", DataType::List(Box::new(DataType::Int64))),
        ]);
        let input = Utf8Array::from_slice("col", &[input_str]);
        let series = deserialize(&input, &dtype).unwrap();
        let output = serialize(series).unwrap();
        assert_eq!(output.len(), 1);
        let round2 = deserialize(&output, &dtype).unwrap();
        assert_eq!(round2.len(), 1);
    }

    #[test]
    fn test_roundtrip_all_nulls() {
        let input = Utf8Array::full_null("col", &DataType::Utf8, 3);
        let series = deserialize(&input, &DataType::Int64).unwrap();
        assert_eq!(series.null_count(), 3);
        let output = serialize(series).unwrap();
        assert_eq!(output.len(), 3);
        assert_eq!(output.null_count(), 3);
    }

    #[test]
    fn test_roundtrip_int64() {
        let input = Utf8Array::from_iter("col", [Some("10"), None, Some("-5"), Some("0")]);
        let series = deserialize(&input, &DataType::Int64).unwrap();
        assert!(series.i64().is_ok());
        let output = serialize(series).unwrap();
        let round2 = deserialize(&output, &DataType::Int64).unwrap();
        let vals: Vec<_> = round2.i64().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(10), None, Some(-5), Some(0)]);
    }

    #[test]
    fn test_roundtrip_boolean() {
        let input = Utf8Array::from_iter("col", [Some("true"), None, Some("false")]);
        let series = deserialize(&input, &DataType::Boolean).unwrap();
        assert!(series.bool().is_ok());
        let output = serialize(series).unwrap();
        let round2 = deserialize(&output, &DataType::Boolean).unwrap();
        let vals: Vec<_> = round2.bool().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn test_serialize_struct() {
        let input =
            Utf8Array::from_slice("col", &[r#"{"a":1,"b":"hello"}"#, r#"{"a":2,"b":"world"}"#]);
        let dtype = DataType::Struct(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let series = deserialize(&input, &dtype).unwrap();
        assert!(series.struct_().is_ok());
        let output = serialize(series).unwrap();
        let vals: Vec<_> = output.into_iter().collect();
        assert_eq!(
            vals,
            vec![
                Some(r#"{"a":1,"b":"hello"}"#),
                Some(r#"{"a":2,"b":"world"}"#)
            ]
        );
    }
}
