use common_error::{ensure, DaftError, DaftResult};
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

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
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

    fn function_args_to_field(
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

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, desc, _nulls_first] => match (data.to_field(schema), desc.to_field(schema)) {
                (Ok(field), Ok(desc_field)) => match (&field.dtype, &desc_field.dtype) {
                    (
                        l @ (DataType::List(_) | DataType::FixedSizeList(_, _)),
                        DataType::Boolean,
                    ) => Ok(Field::new(field.name, l.clone())),
                    (a, b) => Err(DaftError::TypeError(format!(
                        "Expects inputs to list_sort to be list and bool, but received {a} and {b}",
                    ))),
                },
                (Err(e), _) | (_, Err(e)) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, desc, nulls_first] => data.list_sort(desc, nulls_first),
            _ => Err(DaftError::ValueError(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_sort(input: ExprRef, desc: Option<ExprRef>, nulls_first: Option<ExprRef>) -> ExprRef {
    let desc = desc.unwrap_or_else(|| lit(false));
    let nulls_first = nulls_first.unwrap_or_else(|| desc.clone());
    ScalarFunction::new(ListSort {}, vec![input, desc, nulls_first]).into()
}
