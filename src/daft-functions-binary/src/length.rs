use common_error::{ensure, DaftResult};
use daft_core::{
    datatypes::{BinaryArray, DataType, Field, FixedSizeBinaryArray},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF, UnaryArg},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::kernels::BinaryArrayExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryLength;

#[typetag::serde]
impl ScalarUDF for BinaryLength {
    fn name(&self) -> &'static str {
        "binary_length"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        ensure!(
            input.dtype.is_binary() || input.dtype.is_fixed_size_binary(),
            TypeError: "Expects input to length to be binary, but received {input}"
        );

        Ok(Field::new(input.name, DataType::UInt64))
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;
        match input.data_type() {
            DataType::Binary => {
                let binary_array = input.downcast::<BinaryArray>()?;
                let result = binary_array.length()?;
                Ok(result.into_series())
            }
            DataType::FixedSizeBinary(_size) => {
                let binary_array = input.downcast::<FixedSizeBinaryArray>()?;
                let result = binary_array.length()?;
                Ok(result.into_series())
            }
            _ => unreachable!("Type checking is done in to_field"),
        }
    }
}
