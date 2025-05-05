use common_error::DaftResult;
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{BinaryArray, FixedSizeBinaryArray, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::codecs::{Codec, CodecKind};
use crate::invalid_argument_err;

//-----------------------
// methods
//-----------------------

#[must_use]
pub fn decode(input: ExprRef, codec: Codec) -> ExprRef {
    if codec == Codec::Utf8 {
        // special-case for decode('utf-8')
        return input.cast(&DataType::Utf8);
    }
    ScalarFunction::new(Decode { codec }, vec![input]).into()
}

#[must_use]
pub fn try_decode(input: ExprRef, codec: Codec) -> ExprRef {
    // TODO this path requires try_cast, for now use a binary-to-text decode
    // if codec == Codec::Utf8 {
    //     // special-case for try_decode('utf-8')
    //     return input.try_cast(&DataType::Utf8)
    // }
    ScalarFunction::new(TryDecode { codec }, vec![input]).into()
}

//-----------------------
// implementations
//-----------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Decode {
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for Decode {
    fn name(&self) -> &'static str {
        "decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field(inputs, schema, self.codec.returns())
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match &inputs[0].data_type() {
            DataType::Binary => {
                let arg = inputs[0].downcast::<BinaryArray>()?;
                let res = match self.codec.kind() {
                    CodecKind::Binary => arg.transform(self.codec.decoder())?.into_series(),
                    CodecKind::Text => arg.decode(self.codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            DataType::FixedSizeBinary(_) => {
                let arg = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let res = match self.codec.kind() {
                    CodecKind::Binary => arg.transform(self.codec.decoder())?.into_series(),
                    CodecKind::Text => arg.decode(self.codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TryDecode {
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for TryDecode {
    fn name(&self) -> &'static str {
        "try_decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        to_field(inputs, schema, self.codec.returns())
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }

    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs[0].data_type() {
            DataType::Binary => {
                let arg = inputs[0].downcast::<BinaryArray>()?;
                let res = match self.codec.kind() {
                    CodecKind::Binary => arg.try_transform(self.codec.decoder())?.into_series(),
                    CodecKind::Text => arg.try_decode(self.codec.decoder())?.into_series(),
                };
                Ok(res)
            }
            DataType::FixedSizeBinary(_) => {
                let arg = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let res = match self.codec.kind() {
                    CodecKind::Binary => arg.try_transform(self.codec.decoder())?.into_series(),
                    CodecKind::Text => arg.try_decode(self.codec.decoder())?.into_series(),
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

fn to_field(inputs: &[ExprRef], schema: &Schema, returns: DataType) -> DaftResult<Field> {
    if inputs.len() != 1 {
        invalid_argument_err!("Expected 1 argument, found {}", inputs.len())
    }
    let arg = inputs[0].to_field(schema)?;
    if !matches!(arg.dtype, DataType::Binary | DataType::FixedSizeBinary(_)) {
        invalid_argument_err!(
            "Expected argument to be a Binary or FixedSizeBinary, but received {}",
            arg.dtype
        )
    }
    Ok(Field::new(arg.name, returns))
}
