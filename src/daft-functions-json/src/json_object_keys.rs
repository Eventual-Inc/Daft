use std::sync::Arc;

use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use daft_dsl::functions::prelude::*;
use serde_json::Value;

/// Spark-compatible `json_object_keys`.
///
/// Returns the top-level keys of a JSON object as a list of strings.
///
/// Returns `NULL` when:
///   * the input row is `NULL`,
///   * the input cannot be parsed as JSON, or
///   * the parsed JSON is not an object.
///
/// Returns an empty list when the JSON object is empty.
///
/// **Note on key ordering**: keys are returned in **sorted alphabetical order**,
/// not in source insertion order. This differs from Spark's `json_object_keys`,
/// which preserves insertion order. The difference is a consequence of Daft's
/// underlying `serde_json` configuration (`Value::Object` is backed by a
/// `BTreeMap`); do not rely on insertion order in user code.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonObjectKeys;

#[derive(FunctionArgs)]
struct JsonObjectKeysArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for JsonObjectKeys {
    fn name(&self) -> &'static str {
        "json_object_keys"
    }

    fn docstring(&self) -> &'static str {
        "Returns the top-level keys of a JSON object as a list of strings, in sorted \
         alphabetical order. Returns NULL when the input is NULL, cannot be parsed, or \
         is not an object. Note: this differs from Spark, which preserves insertion order."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let JsonObjectKeysArgs { input } = args.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype == DataType::Utf8,
            TypeError: "json_object_keys expects a Utf8 input, got {}",
            input.dtype
        );
        Ok(Field::new(
            input.name,
            DataType::List(Box::new(DataType::Utf8)),
        ))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let JsonObjectKeysArgs { input } = args.try_into()?;
        let arr = input.utf8()?;
        let name = arr.name().to_string();
        let n = arr.len();

        // Flat child of all keys (in order) and per-row offsets.
        let mut flat_keys: Vec<Option<String>> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(n + 1);
        offsets.push(0);
        let mut row_validity: Vec<bool> = Vec::with_capacity(n);

        for opt in arr {
            let mut row_valid = false;
            if let Some(s) = opt
                && let Ok(Value::Object(map)) = serde_json::from_str::<Value>(s)
            {
                row_valid = true;
                for k in map.keys() {
                    flat_keys.push(Some(k.clone()));
                }
            }
            row_validity.push(row_valid);
            offsets.push(flat_keys.len() as i64);
        }

        let child = daft_core::prelude::Utf8Array::from_iter("item", flat_keys.into_iter());
        let child_series = daft_core::series::IntoSeries::into_series(child);

        // Build validity bitmap for the outer list.
        let validity = NullBuffer::from_iter(row_validity);

        let list_field = Arc::new(Field::new(name, DataType::List(Box::new(DataType::Utf8))));
        let list_arr = daft_core::prelude::ListArray::new(
            list_field,
            child_series,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Some(validity),
        );
        Ok(daft_core::series::IntoSeries::into_series(list_arr))
    }
}
