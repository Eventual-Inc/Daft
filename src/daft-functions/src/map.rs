use arrow_buffer::OffsetBuffer;
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, utils::supertype::try_get_collection_supertype};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, prelude::*, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MapFunction;

fn validate_map_args(num_args: usize) -> DaftResult<()> {
    if num_args == 0 {
        return Err(DaftError::ValueError(
            "Cannot call map with no inputs".to_string(),
        ));
    }
    if num_args % 2 != 0 {
        return Err(DaftError::ValueError(format!(
            "map requires alternating key/value inputs; got {num_args} inputs",
        )));
    }
    Ok(())
}

fn interleave_series_by_row(series: &[Series]) -> DaftResult<Series> {
    if series.len() == 1 {
        return Ok(series[0].clone());
    }

    let num_rows = series[0].len();
    let num_entries = series.len();

    let refs = series.iter().collect::<Vec<_>>();
    let stacked = Series::concat(&refs)?;

    let mut take_indices = Vec::with_capacity(num_rows * num_entries);
    for row_idx in 0..num_rows {
        for entry_idx in 0..num_entries {
            take_indices.push((entry_idx * num_rows + row_idx) as u64);
        }
    }

    if take_indices.is_empty() {
        stacked.slice(0, 0)
    } else {
        stacked.take(&UInt64Array::from_vec("idx", take_indices))
    }
}

#[typetag::serde]
impl ScalarUDF for MapFunction {
    fn name(&self) -> &'static str {
        "map"
    }

    fn call(&self, inputs: FunctionArgs<Series>, ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        validate_map_args(inputs.len())?;

        let target_len = ctx.row_count;
        let key_type = try_get_collection_supertype(
            inputs
                .iter()
                .step_by(2)
                .map(|s| s.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        let value_type = try_get_collection_supertype(
            inputs
                .iter()
                .skip(1)
                .step_by(2)
                .map(|s| s.data_type().clone())
                .collect::<Vec<_>>(),
        )?;

        let kv_struct_dtype = DataType::Struct(vec![
            Field::new("key", key_type.clone()),
            Field::new("value", value_type.clone()),
        ]);

        let mut kv_struct_series = Vec::with_capacity(inputs.len() / 2);
        for pair in inputs.chunks(2) {
            let mut key = pair[0].cast(&key_type)?;
            let mut value = pair[1].cast(&value_type)?;

            if key.len() == 1 && target_len > 1 {
                key = key.broadcast(target_len)?;
            }
            if value.len() == 1 && target_len > 1 {
                value = value.broadcast(target_len)?;
            }
            if key.len() != target_len || value.len() != target_len {
                return Err(DaftError::ValueError(format!(
                    "map key/value inputs must be scalar or match row count {target_len}, got key len {} and value len {}",
                    key.len(),
                    value.len(),
                )));
            }

            let kv_struct = StructArray::new(
                Field::new("item", kv_struct_dtype.clone()),
                vec![key.rename("key"), value.rename("value")],
                None,
            )
            .into_series();
            kv_struct_series.push(kv_struct);
        }

        let flat_child = interleave_series_by_row(&kv_struct_series)?;
        let num_entries = kv_struct_series.len();

        // Build a fixed-width list layout directly to avoid O(rows) row-slice allocations.
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat(num_entries).take(target_len));
        let list_array = ListArray::new(
            Field::new("map", DataType::List(Box::new(kv_struct_dtype))),
            flat_child,
            offsets,
            None,
        );

        let map_dtype = DataType::Map {
            key: Box::new(key_type),
            value: Box::new(value_type),
        };

        Ok(MapArray::new(Field::new("map", map_dtype), list_array).into_series())
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        validate_map_args(inputs.len())?;

        let input_fields = inputs
            .iter()
            .map(|expr| expr.to_field(schema))
            .collect::<DaftResult<Vec<_>>>()?;

        let key_type = try_get_collection_supertype(
            input_fields
                .iter()
                .step_by(2)
                .map(|f| f.dtype.clone())
                .collect::<Vec<_>>(),
        )?;
        let value_type = try_get_collection_supertype(
            input_fields
                .iter()
                .skip(1)
                .step_by(2)
                .map(|f| f.dtype.clone())
                .collect::<Vec<_>>(),
        )?;

        Ok(Field::new(
            "map",
            DataType::Map {
                key: Box::new(key_type),
                value: Box::new(value_type),
            },
        ))
    }
}

#[must_use]
pub fn map(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFn::builtin(MapFunction, inputs).into()
}
