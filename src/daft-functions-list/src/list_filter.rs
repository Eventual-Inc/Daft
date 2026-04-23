use arrow::{array::Array, buffer::OffsetBuffer};
use daft_core::{array::ListArray, datatypes::BooleanArray, prelude::AsArrow, series::IntoSeries};
use daft_dsl::functions::prelude::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListFilter;

#[derive(FunctionArgs)]
struct ListFilterArgs<T> {
    input: T,
    expr: T,
}

#[typetag::serde]
impl ScalarUDF for ListFilter {
    fn name(&self) -> &'static str {
        "list_filter"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        // Predicate children are evaluated first, so `mask` is already a flat boolean
        // series aligned 1:1 with `flat_child`. Nulls in the predicate drop the element.
        let ListFilterArgs {
            input: list_arr,
            expr: mask,
        } = inputs.try_into()?;

        let list_arr = list_arr.list()?;
        let arrow_mask = mask.bool()?.as_arrow()?;

        let keep: Vec<bool> = (0..arrow_mask.len())
            .map(|i| arrow_mask.is_valid(i) && arrow_mask.value(i))
            .collect();

        let new_offsets =
            OffsetBuffer::<i64>::from_lengths(list_arr.offsets().windows(2).map(|w| {
                keep[w[0] as usize..w[1] as usize]
                    .iter()
                    .filter(|&&k| k)
                    .count()
            }));

        let keep_mask = BooleanArray::from_values("mask", keep.into_iter());
        let new_child = list_arr.flat_child.filter(&keep_mask)?;

        let res = ListArray::new(
            list_arr.field.clone(),
            new_child,
            new_offsets,
            list_arr.nulls().cloned(),
        );
        Ok(res.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ListFilterArgs { input, expr } = inputs.try_into()?;

        let input_field = input.to_field(schema)?;
        ensure!(
            input_field.dtype.is_list(),
            TypeError: "list_filter expects a list input, got {}",
            input_field.dtype
        );

        let expr_field = expr.to_field(schema)?;
        ensure!(
            expr_field.dtype == DataType::Boolean,
            TypeError: "list_filter predicate must be Boolean, got {}",
            expr_field.dtype
        );

        Ok(input_field.to_exploded_field()?.to_list_field())
    }
}
