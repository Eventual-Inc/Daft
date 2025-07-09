use common_error::{ensure, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{BinaryArray, FixedSizeBinaryArray, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::codecs::{Codec, CodecKind};
use crate::kernels::BinaryArrayExtension;

//-----------------------
// implementations
//-----------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryDecode;

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, common_macros::FunctionArgs,
)]
struct Args<T> {
    input: T,
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for BinaryDecode {
    fn name(&self) -> &'static str {
        "decode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, codec } = inputs.try_into()?;
        let input = input.to_field(schema)?;

        if codec == Codec::Utf8 {
            // special-case for decode('utf-8')
            return Ok(Field::new(input.name, DataType::Utf8));
        }
        ensure!(
            matches!(input.dtype, DataType::Binary | DataType::FixedSizeBinary(_)),
            TypeError: "Expected argument to be a Binary or FixedSizeBinary, but received {}",
            input.dtype
        );

        Ok(Field::new(input.name, codec.returns()))
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, codec } = inputs.try_into()?;
        if codec == Codec::Utf8 {
            // special-case for decode('utf-8')
            return input.cast(&DataType::Utf8);
        }

        match input.data_type() {
            DataType::Binary => {
                let arg = input.downcast::<BinaryArray>()?;
                let res = match codec.kind() {
                    CodecKind::Binary => arg.transform(codec.decoder())?.into_series(),
                    CodecKind::Text => arg.decode(codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            DataType::FixedSizeBinary(_) => {
                let arg = input.downcast::<FixedSizeBinaryArray>()?;
                let res = match codec.kind() {
                    CodecKind::Binary => arg.transform(codec.decoder())?.into_series(),
                    CodecKind::Text => arg.decode(codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryTryDecode;

#[typetag::serde]
impl ScalarUDF for BinaryTryDecode {
    fn name(&self) -> &'static str {
        "try_decode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, codec } = inputs.try_into()?;
        let input = input.to_field(schema)?;

        ensure!(
            matches!(input.dtype, DataType::Binary | DataType::FixedSizeBinary(_)),
            TypeError: "Expected argument to be a Binary or FixedSizeBinary, but received {}",
            input.dtype
        );

        Ok(Field::new(input.name, codec.returns()))
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, codec } = inputs.try_into()?;

        match input.data_type() {
            DataType::Binary => {
                let arg = input.downcast::<BinaryArray>()?;
                let res = match codec.kind() {
                    CodecKind::Binary => arg.try_transform(codec.decoder())?.into_series(),
                    CodecKind::Text => arg.try_decode(codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            DataType::FixedSizeBinary(_) => {
                let arg = input.downcast::<FixedSizeBinaryArray>()?;
                let res = match codec.kind() {
                    CodecKind::Binary => arg.try_transform(codec.decoder())?.into_series(),
                    CodecKind::Text => arg.try_decode(codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}
