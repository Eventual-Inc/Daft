use daft_dsl::functions::prelude::*;
use serde_json::Value;

/// Spark-compatible `json_array_length`.
///
/// Returns the number of elements in the outermost JSON array.
/// Returns `NULL` when:
///   * the input row is `NULL`,
///   * the input cannot be parsed as JSON, or
///   * the parsed JSON is not an array.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonArrayLength;

#[derive(FunctionArgs)]
struct JsonArrayLengthArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for JsonArrayLength {
    fn name(&self) -> &'static str {
        "json_array_length"
    }

    fn docstring(&self) -> &'static str {
        "Returns the number of elements in the outermost JSON array. \
         Returns NULL when the input is NULL, cannot be parsed, or is not an array."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let JsonArrayLengthArgs { input } = args.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype == DataType::Utf8,
            TypeError: "json_array_length expects a Utf8 input, got {}",
            input.dtype
        );
        Ok(Field::new(input.name, DataType::Int32))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let JsonArrayLengthArgs { input } = args.try_into()?;
        let arr = input.utf8()?;
        let name = arr.name().to_string();

        let lens_iter = arr.into_iter().map(|opt| {
            opt.and_then(|s| match serde_json::from_str::<Value>(s) {
                Ok(Value::Array(items)) => i32::try_from(items.len()).ok(),
                _ => None,
            })
        });

        let result =
            daft_core::prelude::Int32Array::from_iter(Field::new(name, DataType::Int32), lens_iter);
        Ok(daft_core::series::IntoSeries::into_series(result))
    }
}
