use daft_core::{array::ListArray, series::IntoSeries};
use daft_dsl::functions::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListMap;

#[derive(FunctionArgs)]
struct ListMapArgs<T> {
    input: T,
    expr: T,
}

#[typetag::serde]

impl ScalarUDF for ListMap {
    fn name(&self) -> &'static str {
        "list_map"
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        // Since we evaluate the children first,
        // the `.element()...` expr is already evaluated by the time this `evaluate` is called here.
        // So we don't really need to perform any work, but instead just put it back in to the list array
        let ListMapArgs {
            input: list_arr,
            expr: result_arr,
        } = inputs.try_into()?;

        let list_arr = list_arr.list()?;
        let offsets = list_arr.offsets();
        let validity = list_arr.validity().cloned();

        let field = result_arr.field().to_list_field()?;

        let res = ListArray::new(field, result_arr, offsets.clone(), validity);
        Ok(res.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ListMapArgs { input, expr } = inputs.try_into()?;

        let input = input.to_field(schema)?;
        let expr = expr.to_field(schema)?;

        Ok(expr.to_list_field()?.rename(input.name))
    }
}
