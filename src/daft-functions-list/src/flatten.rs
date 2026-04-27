use common_error::{DaftResult, ensure};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Flatten;

#[typetag::serde]
impl ScalarUDF for Flatten {
    fn name(&self) -> &'static str {
        "list_flatten"
    }
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_flatten()
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 1,
            SchemaMismatch: "Expected 1 input arg, got {}",
            inputs.len()
        );
        let field = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(
            field.dtype.is_list() || field.dtype.is_fixed_size_list(),
            "Input must be a list"
        );
        let inner_field = field.to_exploded_field()?;
        ensure!(
            inner_field.dtype.is_list() || inner_field.dtype.is_fixed_size_list(),
            "Input must be a list of lists"
        );
        Ok(inner_field.to_exploded_field()?.to_list_field())
    }
}

#[must_use]
pub fn list_flatten(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(Flatten {}, vec![expr]).into()
}