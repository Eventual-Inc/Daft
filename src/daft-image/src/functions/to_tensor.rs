use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageToTensor;

#[derive(FunctionArgs)]
struct ImageToTensorArgs<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for ImageToTensor {
    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ImageToTensorArgs { input } = inputs.try_into()?;
        crate::series::to_tensor(&input)
    }

    fn name(&self) -> &'static str {
        "to_tensor"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageToTensorArgs { input } = inputs.try_into()?;

        let field = input.to_field(schema)?;
        let output_dtype = crate::series::infer_to_tensor_dtype(&field.dtype)?;

        Ok(Field::new(field.name, output_dtype))
    }

    fn docstring(&self) -> &'static str {
        "Converts an image series to a tensor series, inferring dtype and fixed/variable shape."
    }
}
