use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::functions::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Truncate;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    #[arg(optional)]
    relative_to: Option<T>,
    interval: String,
}

#[typetag::serde]
impl ScalarUDF for Truncate {
    fn name(&self) -> &'static str {
        "truncate"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input,
            relative_to,
            interval,
        } = inputs.try_into()?;
        let relative_to = relative_to
            .unwrap_or_else(|| Series::full_null("relative_to", input.data_type(), input.len()));
        input.dt_truncate(&interval, &relative_to)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args {
            input, relative_to, ..
        } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype.is_temporal() ,
            TypeError: "Expected temporal input args, got {}",
            input.dtype
        );

        if let Some(relative_to) = relative_to {
            let relative_to = relative_to.to_field(schema)?;
            ensure!(
                relative_to.dtype.is_null_or(DataType::is_temporal),
                TypeError: "Expected temporal input args, got {}",
                relative_to.dtype
            );
        }
        Ok(Field::new(input.name, input.dtype))
    }
}
