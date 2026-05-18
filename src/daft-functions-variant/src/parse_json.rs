use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use arrow_buffer::NullBuffer;
use daft_core::{
    array::StructArray,
    datatypes::Field,
    prelude::{DataType, Schema, VariantArray},
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

    let mut metadata_builder = LargeBinaryBuilder::with_capacity(utf8_arr.len(), 0);
    let mut value_builder = LargeBinaryBuilder::with_capacity(utf8_arr.len(), 0);
    let mut validity = Vec::with_capacity(utf8_arr.len());

    for val in utf8_arr {
        match val {
            Some(json_str) => {
                let mut builder = VariantBuilder::new();
                builder.append_json(json_str).map_err(|e| {
                    common_error::DaftError::ComputeError(format!("Failed to parse JSON: {e}"))
                })?;
                let (metadata, value) = builder.finish();
                metadata_builder.append_value(&metadata);
                value_builder.append_value(&value);
                validity.push(true);
            }
            None => {
                metadata_builder.append_null();
                value_builder.append_null();
                validity.push(false);
            }
        }
    }

    let metadata_arrow = metadata_builder.finish();
    let value_arrow = value_builder.finish();
    let has_nulls = validity.iter().any(|v| !v);
    let null_buffer = if has_nulls {
        Some(NullBuffer::from_iter(validity))
    } else {
        None
    };

    let value_field = Field::new("value", DataType::Binary);
    let metadata_field = Field::new("metadata", DataType::Binary);

    let value_series = Series::from_arrow(value_field.clone(), Arc::new(value_arrow))?;
    let metadata_series = Series::from_arrow(metadata_field.clone(), Arc::new(metadata_arrow))?;

    let struct_arr = StructArray::new(
        Arc::new(Field::new(
            input.name(),
            DataType::Struct(vec![value_field, metadata_field]),
        )),
        vec![value_series, metadata_series],
        null_buffer,
    );

    Ok(VariantArray::new(field, struct_arr).into_series())
}
