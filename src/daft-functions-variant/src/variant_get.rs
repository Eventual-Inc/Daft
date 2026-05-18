use arrow_array::Array;
use daft_core::{
    datatypes::Field,
    prelude::{AsArrow, DataType, Schema, Utf8Array, VariantArray},
    series::{IntoSeries, Series},
};
use daft_dsl::functions::prelude::*;
use parquet_variant::{Variant, VariantPath};
use parquet_variant_json::VariantToJson as _;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct VariantGet;

#[derive(FunctionArgs)]
struct VariantGetArgs<T> {
    input: T,
    path: String,
}

#[typetag::serde]
impl ScalarUDF for VariantGet {
    fn name(&self) -> &'static str {
        "variant_get"
    }

    fn docstring(&self) -> &'static str {
        "Extracts a value from a Variant by path, returning the result as a JSON string. Path syntax: field.sub[0] (dot notation)."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let VariantGetArgs { input, .. } = args.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Variant, TypeError: "variant_get input must be Variant, got {}", input.dtype);
        Ok(Field::new(input.name, DataType::Utf8))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let VariantGetArgs { input, path } = args.try_into()?;
        variant_get_impl(&input, &path)
    }
}

fn variant_get_impl(input: &Series, path: &str) -> DaftResult<Series> {
    let parsed_path = VariantPath::try_from(path).map_err(|e| {
        common_error::DaftError::ValueError(format!("Invalid variant path '{path}': {e}"))
    })?;

    let variant_arr = input.downcast::<VariantArray>()?;
    let struct_arr = &variant_arr.physical;

    let metadata_series = struct_arr.get("metadata")?;
    let value_series = struct_arr.get("value")?;

    let metadata_arr = metadata_series.binary()?.as_arrow()?;
    let value_arr = value_series.binary()?.as_arrow()?;

    let results: Vec<Option<String>> = (0..input.len())
        .map(|i| {
            if metadata_arr.is_null(i) || value_arr.is_null(i) {
                return Ok(None);
            }
            let metadata_bytes = metadata_arr.value(i);
            let value_bytes = value_arr.value(i);
            let variant = Variant::try_new(metadata_bytes, value_bytes).map_err(|e| {
                common_error::DaftError::ComputeError(format!("Invalid variant: {e}"))
            })?;
            match variant.get_path(&parsed_path) {
                Some(sub_variant) => sub_variant.to_json_string().map(Some).map_err(|e| {
                    common_error::DaftError::ComputeError(format!(
                        "Failed to convert extracted variant to JSON: {e}"
                    ))
                }),
                None => Ok(None),
            }
        })
        .collect::<DaftResult<_>>()?;

    Ok(Utf8Array::from_iter(input.name(), results.into_iter()).into_series())
}
