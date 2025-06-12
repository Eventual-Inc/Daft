use daft_core::{array::ListArray, series::IntoSeries};
use daft_dsl::functions::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListMap;

#[typetag::serde]
impl ScalarUDF for ListMap {
    fn name(&self) -> &'static str {
        "list_map"
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let list_arr = inputs.required(0)?;
        let list_arr = list_arr.list()?;
        let offsets = list_arr.offsets();
        let validity = list_arr.validity().cloned();

        let result_arr = inputs.required(1)?;
        let field = result_arr.field().to_list_field()?;

        let res = ListArray::new(field, result_arr.clone(), offsets.clone(), validity);
        Ok(res.into_series())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let input = inputs.required(0)?.to_field(schema)?;
        let expr = inputs.required(1)?.to_field(schema)?;

        Ok(expr.to_list_field()?.rename(input.name))
    }
}
