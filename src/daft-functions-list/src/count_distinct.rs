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
pub struct ListCountDistinct;

#[typetag::serde]
impl ScalarUDF for ListCountDistinct {
    fn name(&self) -> &'static str {
        "list_count_distinct"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_count_distinct()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let field = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            field.dtype.is_list() || field.dtype.is_fixed_size_list(),
            "Input must be a list"
        );
        Ok(Field::new(field.name, DataType::UInt64))
    }
}

#[must_use]
pub fn list_count_distinct(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListCountDistinct, vec![expr]).into()
}
