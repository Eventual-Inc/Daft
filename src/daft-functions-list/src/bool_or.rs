use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::{
    series::SeriesListExtension,
    utils::{unary_list_evaluate, unary_list_to_field},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListBoolOr;

#[typetag::serde]
impl ScalarUDF for ListBoolOr {
    fn name(&self) -> &'static str {
        "list_bool_or"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        unary_list_evaluate(inputs, Series::list_bool_or)
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_list_to_field(inputs, schema, DataType::Boolean)
    }
}

#[must_use]
pub fn list_bool_or(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListBoolOr, vec![expr]).into()
}
