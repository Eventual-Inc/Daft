use common_error::DaftResult;
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{BinaryArray, FixedSizeBinaryArray, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::codecs::Codec;
use crate::invalid_argument_err;

#[must_use]
pub fn encode(input: ExprRef, codec: Codec) -> ExprRef {
    ScalarFunction::new(Encode { codec }, vec![input]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Encode {
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for Encode {
    fn name(&self) -> &'static str {
        "encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 1 {
            invalid_argument_err!("Expected 1 argument, found {}", inputs.len())
        }
        let arg = inputs[0].to_field(schema)?;
        if !matches!(
            arg.dtype,
            DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
        ) {
            invalid_argument_err!(
                "Expected argument to be Utf8, Binary or FixedSizeBinary, but received {}",
                arg.dtype
            )
        }
        Ok(Field::new(arg.name, DataType::Binary))
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs[0].data_type() {
            DataType::Binary => {
                let arg = inputs[0].downcast::<BinaryArray>()?;
                let res = arg.transform(self.codec.encoder())?;
                Ok(res.into_series())
            }
            DataType::FixedSizeBinary(_) => {
                let arg = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let res = arg.transform(self.codec.encoder())?;
                Ok(res.into_series())
            }
            DataType::Utf8 => {
                if self.codec == Codec::Utf8 {
                    return inputs[0].cast(&DataType::Binary);
                }
                let arg = inputs[0].downcast::<Utf8Array>()?;
                let res = arg.encode(self.codec.encoder())?;
                Ok(res.into_series())
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}

#[must_use]
pub fn try_encode(input: ExprRef, codec: Codec) -> ExprRef {
    ScalarFunction::new(Encode { codec }, vec![input]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryEncode {
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for TryEncode {
    fn name(&self) -> &'static str {
        "try_encode"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 1 {
            invalid_argument_err!("Expected 1 argument, found {}", inputs.len())
        }
        let arg = inputs[0].to_field(schema)?;
        if !matches!(
            arg.dtype,
            DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
        ) {
            invalid_argument_err!(
                "Expected argument to be Utf8, Binary or FixedSizeBinary, but received {}",
                arg.dtype
            )
        }
        Ok(Field::new(arg.name, DataType::Binary))
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs[0].data_type() {
            DataType::Binary => {
                let arg = inputs[0].downcast::<BinaryArray>()?;
                let res = arg.try_transform(self.codec.encoder())?;
                Ok(res.into_series())
            }
            DataType::FixedSizeBinary(_) => {
                let arg = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let res = arg.try_transform(self.codec.encoder())?;
                Ok(res.into_series())
            }
            DataType::Utf8 => {
                if self.codec == Codec::Utf8 {
                    return inputs[0].cast(&DataType::Binary);
                }
                let arg = inputs[0].downcast::<Utf8Array>()?;
                let res = arg.try_encode(self.codec.encoder())?;
                Ok(res.into_series())
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}
