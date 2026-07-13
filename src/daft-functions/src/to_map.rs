use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, utils::supertype::try_get_collection_supertype};
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
        ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call map with no inputs".to_string(),
            ));
        }
        if !inputs.len().is_multiple_of(2) {
            return Err(DaftError::ValueError(
                "Map constructor requires an even number of key/value inputs".to_string(),
            ));
        }

        let target_len = ctx.row_count;
        let inputs = inputs
            .into_iter()
            .map(|s| {
                if s.len() == 1 && target_len > 1 {
                    s.broadcast(target_len)
                } else {
                    Ok(s)
                }
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let key_dtype =
            try_get_collection_supertype(inputs.iter().step_by(2).map(|s| s.data_type()))?;
        let value_dtype =
            try_get_collection_supertype(inputs.iter().skip(1).step_by(2).map(|s| s.data_type()))?;
        let struct_field = Field::new(
            "entries",
            DataType::Struct(vec![
                Field::new("key", key_dtype.clone()),
                Field::new("value", value_dtype.clone()),
            ]),
        );

        let entries = inputs
            .chunks_exact(2)
            .map(|pair| {
                Ok(StructArray::new(
                    struct_field.clone(),
                    vec![
                        pair[0].cast(&key_dtype)?.rename("key"),
                        pair[1].cast(&value_dtype)?.rename("value"),
                    ],
                    None,
                )
                .into_series())
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let list_field = Field::new("map", DataType::List(Box::new(struct_field.dtype.clone())));
        let entry_refs = entries.iter().collect::<Vec<_>>();
        let physical = Series::zip(list_field, &entry_refs)?.list()?.clone();
        let map_field = Field::new(
            "map",
            DataType::Map {
                key: Box::new(key_dtype),
                value: Box::new(value_dtype),
            },
        );

        Ok(MapArray::new(map_field, physical).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        if inputs.is_empty() {
            return Err(DaftError::ValueError(
                "Cannot call map with no inputs".to_string(),
            ));
        }
        if !inputs.len().is_multiple_of(2) {
            return Err(DaftError::ValueError(
                "Map constructor requires an even number of key/value inputs".to_string(),
            ));
        }

        let input_fields = inputs
            .iter()
            .map(|e| e.to_field(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let key_dtype =
            try_get_collection_supertype(input_fields.iter().step_by(2).map(|f| &f.dtype))?;
        let value_dtype =
            try_get_collection_supertype(input_fields.iter().skip(1).step_by(2).map(|f| &f.dtype))?;

        Ok(Field::new(
            "map",
            DataType::Map {
                key: Box::new(key_dtype),
                value: Box::new(value_dtype),
            },
        ))
    }
}

#[must_use]
pub fn to_map(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFn::builtin(ToMapFunction, inputs).into()
}
