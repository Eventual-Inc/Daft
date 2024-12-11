use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field, Schema, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct ListValueCountsFunction;

#[typetag::serde]
impl ScalarUDF for ListValueCountsFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_value_counts"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let [data] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        let data_field = data.to_field(schema)?;

        let inner_type = match &data_field.dtype {
            DataType::List(inner_type) => inner_type,
            DataType::FixedSizeList(inner_type, _) => inner_type,
            _ => {
                return Err(DaftError::TypeError(format!(
                    "Expected list or fixed size list, got {}",
                    data_field.dtype
                )));
            }
        };

        let map_type = DataType::Map {
            key: inner_type.clone(),
            value: Box::new(DataType::UInt64),
        };

        Ok(Field::new(data_field.name, map_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let [data] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        data.list_value_counts()
    }
}

pub fn list_value_counts(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListValueCountsFunction, vec![expr]).into()
}
