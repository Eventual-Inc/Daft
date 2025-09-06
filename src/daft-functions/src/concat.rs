use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Concat;

#[derive(FunctionArgs)]
pub struct ConcatArgs<T> {
    pub left: T,
    pub right: T,
}

#[typetag::serde]
impl ScalarUDF for Concat {
    fn name(&self) -> &'static str {
        "concat"
    }

    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let ConcatArgs { left, right } = inputs.try_into()?;

        left + right
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ConcatArgs { left, right } = inputs.try_into()?;

        let lfield = left.to_field(schema)?;
        let rfield = right.to_field(schema)?;

        let dtype = match (lfield.dtype, rfield.dtype) {
            (DataType::Utf8, DataType::Utf8) => DataType::Utf8,
            (DataType::Binary, DataType::Binary)
            | (DataType::Binary, DataType::FixedSizeBinary(_))
            | (DataType::FixedSizeBinary(_), DataType::Binary) => DataType::Binary,
            (DataType::FixedSizeBinary(l), DataType::FixedSizeBinary(r)) => {
                DataType::FixedSizeBinary(l + r)
            }
            (l, r) => {
                return Err(DaftError::TypeError(format!(
                    "Expected both arguments to 'concat' function to be utf8 or binary, but received {} and {}",
                    l, r
                )));
            }
        };

        Ok(Field::new(lfield.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Concatenates two string or binary values"
    }
}
