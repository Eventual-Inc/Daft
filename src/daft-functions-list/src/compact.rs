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
pub struct ListCompact;

#[typetag::serde]
impl ScalarUDF for ListCompact {
    fn name(&self) -> &'static str {
        "list_compact"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_compact"]
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        input.list_compact()
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

/// Removes null values from each list, preserving the order of the remaining elements.
/// Spark-compatible alias: `array_compact`.
#[must_use]
pub fn list_compact(expr: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListCompact {}, vec![expr]).into()
}
