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
pub struct Explode;

#[typetag::serde]
impl ScalarUDF for Explode {
    fn name(&self) -> &'static str {
        "explode"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["unnest"]
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.explode()
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
        field.to_exploded_field()
    }
}

#[must_use]
pub fn explode(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(Explode {}, vec![expr]).into()
}
