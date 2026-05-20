use std::collections::HashSet;

use arrow::buffer::NullBuffer;
use daft_core::lit::FromLiteral;
use daft_dsl::functions::prelude::*;
use serde_json::Value;

/// Spark-compatible `json_tuple`.
///
/// Extracts the values for the specified set of top-level keys from a JSON
/// object string. Spark returns one column per key (`c0`, `c1`, ...); to fit
/// Daft's single-output UDF model, we return a `Struct` whose field names are
/// the requested keys, each typed as `Utf8`.
///
/// Behavior:
///   * Non-string scalar values (numbers, booleans) are stringified without
///     quotes (e.g. `1`, `true`).
///   * Nested objects/arrays are returned as their JSON-encoded string.
///   * Missing keys yield `NULL` for that field only; the row itself is still
///     valid as long as the input is a parseable JSON object.
///   * Malformed JSON, non-object roots and `NULL` inputs yield a row-level
///     `NULL` (`is_null()` returns `True`); every child field is also `NULL`.
///   * Duplicate field names are rejected at planning time.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JsonTuple;

#[derive(FunctionArgs)]
struct JsonTupleArgs<T> {
    input: T,
    #[arg(variadic)]
    fields: Vec<T>,
}

fn check_unique_keys(keys: &[String]) -> DaftResult<()> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(keys.len());
    for k in keys {
        if !seen.insert(k.as_str()) {
            return Err(common_error::DaftError::ValueError(format!(
                "json_tuple field names must be unique, but '{k}' was specified more than once"
            )));
        }
    }
    Ok(())
}

fn extract_keys_from_exprs(fields: &[ExprRef]) -> DaftResult<Vec<String>> {
    if fields.is_empty() {
        return Err(common_error::DaftError::ValueError(
            "json_tuple requires at least one field name".to_string(),
        ));
    }
    let keys: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let lit = e.as_literal().ok_or_else(|| {
                common_error::DaftError::ValueError(format!(
                    "json_tuple field name at position {} must be a string literal",
                    i + 1
                ))
            })?;
            String::try_from_literal(lit).map_err(|_| {
                common_error::DaftError::ValueError(format!(
                    "json_tuple field name at position {} must be a string literal",
                    i + 1
                ))
            })
        })
        .collect::<DaftResult<Vec<_>>>()?;
    check_unique_keys(&keys)?;
    Ok(keys)
}

fn extract_keys_from_series(fields: &[Series]) -> DaftResult<Vec<String>> {
    let keys: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let arr = s.utf8().map_err(|_| {
                common_error::DaftError::ValueError(format!(
                    "json_tuple field name at position {} must be a string literal",
                    i + 1
                ))
            })?;
            if arr.len() != 1 {
                return Err(common_error::DaftError::ValueError(format!(
                    "json_tuple field name at position {} must be a string literal (got an array of length {})",
                    i + 1, arr.len()
                )));
            }
            arr
                .into_iter()
                .next()
                .flatten()
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    common_error::DaftError::ValueError(format!(
                        "json_tuple field name at position {} must be a non-null string literal",
                        i + 1
                    ))
                })
        })
        .collect::<DaftResult<Vec<_>>>()?;
    check_unique_keys(&keys)?;
    Ok(keys)
}

/// Convert a JSON value to its Spark-style string representation:
///   * String -> raw inner value (no surrounding quotes)
///   * Other scalars -> their JSON literal form
///   * Objects/arrays -> compact JSON-encoded string
fn json_value_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[typetag::serde]
impl ScalarUDF for JsonTuple {
    fn name(&self) -> &'static str {
        "json_tuple"
    }

    fn docstring(&self) -> &'static str {
        "Extracts the values for the given top-level keys from a JSON object string and \
         returns them as a Struct. Equivalent to Spark's json_tuple but returns a struct \
         (with field names = keys) instead of multiple columns. Field names must be \
         unique. The struct row is NULL when the input is NULL, malformed, or not an \
         object; missing keys produce field-level NULLs."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let JsonTupleArgs { input, fields } = args.try_into()?;
        let input_field = input.to_field(schema)?;
        ensure!(
            input_field.dtype == DataType::Utf8,
            TypeError: "json_tuple expects a Utf8 first argument, got {}",
            input_field.dtype
        );

        let keys = extract_keys_from_exprs(&fields)?;
        let struct_fields: Vec<Field> = keys
            .iter()
            .map(|k| Field::new(k.as_str(), DataType::Utf8))
            .collect();
        Ok(Field::new(
            input_field.name,
            DataType::Struct(struct_fields),
        ))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let JsonTupleArgs { input, fields } = args.try_into()?;
        let keys = extract_keys_from_series(&fields)?;

        let arr = input.utf8()?;
        let name = arr.name().to_string();
        let n = arr.len();

        // Per-key column buffers + per-row validity for the outer struct.
        let mut columns: Vec<Vec<Option<String>>> =
            (0..keys.len()).map(|_| Vec::with_capacity(n)).collect();
        let mut row_validity: Vec<bool> = Vec::with_capacity(n);

        for opt in arr {
            let parsed = opt.and_then(|s| serde_json::from_str::<Value>(s).ok());
            match parsed {
                Some(Value::Object(map)) => {
                    row_validity.push(true);
                    for (i, k) in keys.iter().enumerate() {
                        columns[i].push(map.get(k).map(json_value_to_str));
                    }
                }
                _ => {
                    row_validity.push(false);
                    for col in &mut columns {
                        col.push(None);
                    }
                }
            }
        }

        // Build child Utf8 series and the resulting Struct.
        let child_series: Vec<Series> = keys
            .iter()
            .zip(columns.into_iter())
            .map(|(k, col)| {
                let arr = daft_core::prelude::Utf8Array::from_iter(k.as_str(), col.into_iter());
                daft_core::series::IntoSeries::into_series(arr)
            })
            .collect();

        let struct_fields: Vec<Field> = child_series.iter().map(|s| s.field().clone()).collect();
        let struct_field = Field::new(name, DataType::Struct(struct_fields));
        let validity = NullBuffer::from_iter(row_validity);
        let struct_arr =
            daft_core::prelude::StructArray::new(struct_field, child_series, Some(validity));
        Ok(daft_core::series::IntoSeries::into_series(struct_arr))
    }
}
