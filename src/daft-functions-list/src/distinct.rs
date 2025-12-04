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
pub struct ListDistinct;

#[typetag::serde]
impl ScalarUDF for ListDistinct {
    fn name(&self) -> &'static str {
        "list_distinct"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_distinct()
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
        let inner_type = field.dtype.dtype().unwrap().clone();
        Ok(Field::new(field.name, DataType::List(Box::new(inner_type))))
    }
}

/// Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.
pub fn list_distinct(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListDistinct {}, vec![expr]).into()
}
