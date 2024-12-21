use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field, Schema, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct ListDistinctFunction;

#[typetag::serde]
impl ScalarUDF for ListDistinctFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_distinct"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let [data] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        let data_field = data.to_field(schema)?;

        let DataType::List(inner_type) = &data_field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected list, got {}",
                data_field.dtype
            )));
        };

        let list_type = DataType::List(inner_type.clone());

        Ok(Field::new(data_field.name, list_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let [data] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        data.list_distinct()
    }
}

pub fn list_distinct(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListDistinctFunction, vec![expr]).into()
}
