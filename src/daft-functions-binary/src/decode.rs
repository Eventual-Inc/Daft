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
pub struct DecodeArgs<T> {
    input: T,
    codec: T,
}

#[typetag::serde]
impl ScalarUDF for BinaryDecode {
    fn name(&self) -> &'static str {
        "binary_decode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        to_field_impl(inputs, schema)
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let codec: Codec = inputs.extract((1, "codec"))?;

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
        "binary_try_decode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        to_field_impl(inputs, schema)
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let codec: Codec = inputs.extract((1, "codec"))?;
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

//---------
// helpers
//----------

fn to_field_impl(inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
    ensure!(inputs.len() == 2, TypeError: "Expected 2 arguments, found {}", inputs.len());
    let input = inputs.required((0, "input"))?.to_field(schema)?;
    let codec: Codec = inputs.extract((1, "codec"))?;

    ensure!(
        matches!(input.dtype, DataType::Binary | DataType::FixedSizeBinary(_)),
        TypeError: "Expected argument to be a Binary or FixedSizeBinary, but received {}",
        input.dtype
    );

    Ok(Field::new(input.name, codec.returns()))
}
