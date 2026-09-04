use common_error::{DaftResult, ensure};
use daft_core::{
    datatypes::DataType,
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
pub struct ListPosition;

#[typetag::serde]
impl ScalarUDF for ListPosition {
    fn name(&self) -> &'static str {
        "list_position"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["array_position"]
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let list_series = inputs.required((0, "list"))?;
        let item = inputs.required((1, "item"))?;

        // Broadcast scalar item to match list length when needed.
        let item = if item.len() == 1 {
            &item.broadcast(list_series.len())?
        } else {
            item
        };

        ensure!(
            item.len() == list_series.len(),
            ValueError: "Item length must match list length"
        );

        list_series.list_position(item)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 input args, got {}",
            inputs.len()
        );

        let list_field = inputs.required((0, "list"))?.to_field(schema)?;
        let item_field = inputs.required((1, "item"))?.to_field(schema)?;

        ensure!(
            list_field.dtype.is_list() || list_field.dtype.is_fixed_size_list(),
            TypeError: "First argument must be a list, got {}",
            list_field.dtype
        );

        let list_element_field = list_field.to_exploded_field()?;
        if !list_element_field.dtype.is_null() {
            ensure!(
                list_element_field.dtype == item_field.dtype || item_field.dtype.is_null(),
                TypeError: "Cannot search for item of type {} in list of type {}",
                item_field.dtype,
                list_element_field.dtype
            );
        }

        Ok(Field::new(list_field.name, DataType::Int64))
    }
}

/// Returns the 1-based position of the first occurrence of `item` in each list.
///
/// Returns 0 if the element is not found, or NULL if either the list or item is NULL.
/// Spark-compatible alias: `array_position`.
#[must_use]
pub fn list_position(list_expr: ExprRef, item: ExprRef) -> ExprRef {
    ScalarFn::builtin(ListPosition {}, vec![list_expr, item]).into()
}
