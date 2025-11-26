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
pub struct ListGet;

#[typetag::serde]
impl ScalarUDF for ListGet {
    fn name(&self) -> &'static str {
        "list_get"
    }

    fn call(&self, args: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = args.required((0, "input"))?;
        let idx = args.required((1, "index"))?;
        let _default = args.required((2, "default"))?;
        input.list_get(idx, _default)
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        ensure!(
            args.len() == 3,
            SchemaMismatch: "Expected 3 input args, got {}",
            args.len()
        );

        let input = args.required((0, "input"))?.to_field(schema)?;
        let idx = args.required(1)?.to_field(schema)?;
        let _default = args.required(2)?.to_field(schema)?;

        ensure!(
            input.dtype.is_list() || input.dtype.is_fixed_size_list(),
            "Input must be a list"
        );

        ensure!(
            idx.dtype.is_integer(),
            TypeError: "Index must be an integer, received: {}",
            idx.dtype
        );

        // TODO(Kevin): Check if default dtype can be cast into input dtype.
        let exploded_field = input.to_exploded_field()?;
        Ok(exploded_field)
    }
}

#[must_use]
pub fn list_get(expr: ExprRef, idx: ExprRef, default_value: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListGet {}, vec![expr, idx, default_value]).into()
}
