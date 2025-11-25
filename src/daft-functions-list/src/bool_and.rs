use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListBoolAnd;

#[typetag::serde]
impl ScalarUDF for ListBoolAnd {
    fn name(&self) -> &'static str {
        "list_bool_and"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_bool_and()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let inner_field = input.to_exploded_field()?;

        Ok(Field::new(inner_field.name.as_str(), DataType::Boolean))
    }
}

#[must_use]
pub fn list_bool_and(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListBoolAnd, vec![expr]).into()
}
