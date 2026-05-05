use arrow_buffer::OffsetBuffer;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct ToMapFunction;

#[typetag::serde]
impl ScalarUDF for ToMapFunction {
    fn name(&self) -> &'static str {
        "map"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        if inputs.is_empty() || !inputs.len().is_multiple_of(2) {
            return Err(DaftError::ValueError(
                "to_map requires an even number of inputs (key-value pairs)".to_string(),
            ));
        }

        let num_pairs = inputs.len() / 2;
        let num_rows = inputs.iter().map(|s| s.len()).max().unwrap();

        let inputs: Vec<Series> = inputs
            .into_iter()
            .map(|s| {
                if s.len() == 1 && num_rows > 1 {
                    s.broadcast(num_rows)
                } else if s.len() != num_rows {
                    Err(DaftError::ValueError(format!(
                        "All inputs to to_map must have the same length or be length 1, got {}",
                        s.len()
                    )))
                } else {
                    Ok(s)
                }
            })
            .collect::<DaftResult<_>>()?;

        let keys: Vec<&Series> = inputs.iter().step_by(2).collect();
        let values: Vec<&Series> = inputs.iter().skip(1).step_by(2).collect();

        let key_dtype = keys[0].data_type();
        for k in &keys[1..] {
            if k.data_type() != key_dtype {
                return Err(DaftError::ValueError(format!(
                    "All keys in to_map must have the same type. Expected {}, got {}",
                    key_dtype,
                    k.data_type()
                )));
            }
        }

        let value_dtype = values[0].data_type();
        for v in &values[1..] {
            if v.data_type() != value_dtype {
                return Err(DaftError::ValueError(format!(
                    "All values in to_map must have the same type. Expected {}, got {}",
                    value_dtype,
                    v.data_type()
                )));
            }
        }

        // Concat all keys and all values (grouped by pair).
        // Then transpose from (num_pairs, num_rows) to (num_rows, num_pairs)
        // so each row's entries are contiguous in the flat child.
        let concat_keys = Series::concat(&keys)?;
        let concat_values = Series::concat(&values)?;

        let total = num_pairs * num_rows;
        let indices: Vec<u64> = (0..total)
            .map(|p| {
                let row = p / num_pairs;
                let pair = p % num_pairs;
                (pair * num_rows + row) as u64
            })
            .collect();

        let idx = UInt64Array::from_vec("idx", indices);
        let flat_keys = concat_keys.take(&idx)?.rename("key");
        let flat_values = concat_values.take(&idx)?.rename("value");

        let struct_field = Field::new(
            "entries",
            DataType::Struct(vec![
                Field::new("key", key_dtype.clone()),
                Field::new("value", value_dtype.clone()),
            ]),
        );
        let struct_array = StructArray::new(struct_field, vec![flat_keys, flat_values], None);

        let offsets: Vec<i64> = (0..=num_rows).map(|i| (i * num_pairs) as i64).collect();
        let list_field = Field::new(
            "map",
            DataType::List(Box::new(DataType::Struct(vec![
                Field::new("key", key_dtype.clone()),
                Field::new("value", value_dtype.clone()),
            ]))),
        );
        let list_array = ListArray::new(
            list_field,
            struct_array.into_series(),
            OffsetBuffer::new(offsets.into()),
            None,
        );

        let map_field = Field::new(
            "map",
            DataType::Map {
                key: Box::new(key_dtype.clone()),
                value: Box::new(value_dtype.clone()),
            },
        );
        Ok(MapArray::new(map_field, list_array).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        if inputs.is_empty() || !inputs.len().is_multiple_of(2) {
            return Err(DaftError::ValueError(
                "to_map requires an even number of inputs (key-value pairs)".to_string(),
            ));
        }

        let key_field = inputs[0].to_field(schema)?;
        let value_field = inputs[1].to_field(schema)?;

        for i in (2..inputs.len()).step_by(2) {
            let f = inputs[i].to_field(schema)?;
            if f.dtype != key_field.dtype {
                return Err(DaftError::ValueError(format!(
                    "All keys in to_map must have the same type. Expected {}, got {}",
                    key_field.dtype, f.dtype
                )));
            }
        }

        for i in (3..inputs.len()).step_by(2) {
            let f = inputs[i].to_field(schema)?;
            if f.dtype != value_field.dtype {
                return Err(DaftError::ValueError(format!(
                    "All values in to_map must have the same type. Expected {}, got {}",
                    value_field.dtype, f.dtype
                )));
            }
        }

        Ok(Field::new(
            "map",
            DataType::Map {
                key: Box::new(key_field.dtype),
                value: Box::new(value_field.dtype),
            },
        ))
    }
}

#[must_use]
pub fn to_map(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFn::builtin(ToMapFunction, inputs).into()
}
