use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{functions::FunctionArgs, ExprRef};

pub(crate) fn unary_list_evaluate(
    inputs: daft_dsl::functions::FunctionArgs<Series>,
    f: impl Fn(&Series) -> DaftResult<Series>,
) -> DaftResult<Series> {
    let input = inputs.required((0, "input"))?;
    if input.data_type().is_null() {
        return Ok(Series::full_null(
            input.name(),
            &DataType::Null,
            input.len(),
        ));
    }
    f(input)
}
pub(crate) fn unary_list_to_field(
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
    return_dtype: DataType,
) -> DaftResult<Field> {
    ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
    let input = inputs.required((0, "input"))?.to_field(schema)?;

    if input.dtype.is_null() {
        Ok(Field::new(input.name, DataType::Null))
    } else {
        let inner_field = input.to_exploded_field()?;

        Ok(Field::new(inner_field.name.as_str(), return_dtype))
    }
}
