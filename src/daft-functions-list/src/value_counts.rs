use common_error::{ensure, DaftResult};
use daft_core::prelude::{DataType, Field, Schema, Series};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListValueCounts;

#[typetag::serde]
impl ScalarUDF for ListValueCounts {
    fn name(&self) -> &'static str {
        "list_value_counts"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 1, ValueError: "Expected 1 input arg, got {}", inputs.len());
        let input = inputs.required((0, "input"))?;
        input.list_value_counts()
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input arg, got {}", inputs.len());
        let field = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(field.dtype.is_list() || field.dtype.is_fixed_size_list(), TypeError: "Expected input to be list or fixed size list, got {}", field.dtype);

        let inner_type = field.dtype.dtype().unwrap();

        let map_type = DataType::Map {
            key: Box::new(inner_type.clone()),
            value: Box::new(DataType::UInt64),
        };

        Ok(Field::new(field.name, map_type))
    }
}

pub fn list_value_counts(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListValueCounts, vec![expr]).into()
}
