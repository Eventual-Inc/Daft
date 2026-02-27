use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{binary_utf8_evaluate, binary_utf8_to_field, utf8_compare_op};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Like;

#[typetag::serde]
impl ScalarUDF for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            utf8_compare_op(s, pattern, arrow::compute::kernels::comparison::like)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::Boolean,
        )
    }

    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether string matches the given pattern. (case-sensitive)"
    }
}

#[must_use]
pub fn like(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFn::builtin(Like {}, vec![input, pattern]).into()
}
