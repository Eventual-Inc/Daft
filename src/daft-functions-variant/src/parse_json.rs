use std::sync::Arc;

use daft_core::{
    array::StructArray,
    datatypes::Field,
    prelude::{BinaryArray, DataType, Schema, VariantArray},
    series::{IntoSeries, Series},
};
use daft_dsl::functions::prelude::*;
use parquet_variant::VariantBuilder;
use parquet_variant_json::JsonToVariant;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ParseJson;

#[derive(FunctionArgs)]
struct ParseJsonArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for ParseJson {
    fn name(&self) -> &'static str {
        "variant_parse_json"
    }

    fn docstring(&self) -> &'static str {
        "Parses a JSON string into a Variant binary encoding."
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let ParseJsonArgs { input } = args.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(input.dtype == DataType::Utf8, TypeError: "parse_json input must be a string type, got {}", input.dtype);
        Ok(Field::new(input.name, DataType::Variant))
    }

    fn call(
        &self,
        args: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ParseJsonArgs { input } = args.try_into()?;
        parse_json_impl(&input)
    }
}

fn parse_json_impl(input: &Series) -> DaftResult<Series> {
    let utf8_arr = input.utf8()?;
    let field = Arc::new(Field::new(input.name(), DataType::Variant));

    let mut metadata_values: Vec<Option<&[u8]>> = Vec::with_capacity(utf8_arr.len());
    let mut value_values: Vec<Option<&[u8]>> = Vec::with_capacity(utf8_arr.len());
    let mut metadata_bufs: Vec<Vec<u8>> = Vec::with_capacity(utf8_arr.len());
    let mut value_bufs: Vec<Vec<u8>> = Vec::with_capacity(utf8_arr.len());

    for val in utf8_arr {
        match val {
            Some(json_str) => {
                let mut builder = VariantBuilder::new();
                builder.append_json(json_str).map_err(|e| {
                    common_error::DaftError::ComputeError(format!("Failed to parse JSON: {e}"))
                })?;
                let (metadata, value) = builder.finish();
                metadata_bufs.push(metadata);
                value_bufs.push(value);
            }
            None => {
                metadata_bufs.push(Vec::new());
                value_bufs.push(Vec::new());
            }
        }
    }

    for (i, val) in utf8_arr.into_iter().enumerate() {
        if val.is_some() {
            metadata_values.push(Some(&metadata_bufs[i]));
            value_values.push(Some(&value_bufs[i]));
        } else {
            metadata_values.push(None);
            value_values.push(None);
        }
    }

    let metadata_field = Field::new("metadata", DataType::Binary);
    let value_field = Field::new("value", DataType::Binary);

    let metadata_series =
        BinaryArray::from_iter(metadata_field.name.as_ref(), metadata_values.into_iter())
            .into_series();
    let value_series =
        BinaryArray::from_iter(value_field.name.as_ref(), value_values.into_iter()).into_series();

    let struct_arr = StructArray::new(
        Arc::new(Field::new(
            input.name(),
            DataType::Struct(vec![metadata_field, value_field]),
        )),
        vec![metadata_series, value_series],
        None,
    );

    Ok(VariantArray::new(field, struct_arr).into_series())
}
