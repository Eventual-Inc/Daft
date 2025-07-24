use common_error::{ensure, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    lit, literals_to_series, ExprRef, Literal,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListSort;

#[typetag::serde]
impl ScalarUDF for ListSort {
    fn name(&self) -> &'static str {
        "list_sort"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let data = inputs.required((0, "input"))?;

        let desc = inputs
            .optional((1, "desc"))?
            .cloned()
            .unwrap_or_else(|| literals_to_series(&[false.literal_value()]).unwrap());

        let nulls_first = inputs
            .optional((2, "nulls_first"))?
            .cloned()
            .unwrap_or_else(|| desc.clone());

        data.list_sort(&desc, &nulls_first)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let data = inputs.required((0, "input"))?.to_field(schema)?;
        if let Some(desc) = inputs
            .optional((1, "desc"))?
            .map(|expr| expr.to_field(schema))
            .transpose()?
        {
            ensure!(desc.dtype == DataType::Boolean, "desc must be boolean");
        }
        if let Some(nulls_first) = inputs
            .optional((2, "nulls_first"))?
            .map(|expr| expr.to_field(schema))
            .transpose()?
        {
            ensure!(
                nulls_first.dtype == DataType::Boolean,
                "nulls_first must be boolean"
            );
        }
        Ok(data)
    }
}

#[must_use]
pub fn list_sort(input: ExprRef, desc: Option<ExprRef>, nulls_first: Option<ExprRef>) -> ExprRef {
    let desc = desc.unwrap_or_else(|| lit(false));
    let nulls_first = nulls_first.unwrap_or_else(|| desc.clone());
    ScalarFunction::new(ListSort {}, vec![input, desc, nulls_first]).into()
}
