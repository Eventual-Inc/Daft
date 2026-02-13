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
pub struct StartsWith;

#[typetag::serde]
impl ScalarUDF for StartsWith {
    fn name(&self) -> &'static str {
        "starts_with"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", |s, pattern| {
            utf8_compare_op(s, pattern, arrow::compute::kernels::comparison::starts_with)
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
        "Returns a boolean indicating whether each string starts with the specified pattern"
    }
}

#[must_use]
pub fn startswith(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFn::builtin(StartsWith {}, vec![input, pattern]).into()
}
