use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListBoolOr;

#[typetag::serde]
impl ScalarUDF for ListBoolOr {
    fn name(&self) -> &'static str {
        "list_bool_or"
    }

    fn call(&self, args: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = args.required((0, "input"))?;
        input.list_bool_or()
    }

    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        ensure!(args.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", args.len());
        let input = args.required((0, "input"))?.to_field(schema)?;
        let inner_field = input.to_exploded_field()?;

        Ok(Field::new(inner_field.name.as_str(), DataType::Boolean))
    }
}

#[must_use]
pub fn list_bool_or(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListBoolOr, vec![expr]).into()
}
