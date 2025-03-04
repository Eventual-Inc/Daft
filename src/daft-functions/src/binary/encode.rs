use common_error::DaftResult;
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::codecs::Codec;
use crate::invalid_argument_err;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Encode(Codec);

#[typetag::serde]
impl ScalarUDF for Encode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "encode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 1 {
            invalid_argument_err!("Expected 1 argument, found {}", inputs.len())
        }
        let arg = inputs[0].to_field(schema)?;
        if !matches!(arg.dtype, DataType::Utf8) {
            invalid_argument_err!(
                "Expected argument to be a String, but received {}",
                arg.dtype
            )
        }
        Ok(Field::new(arg.name, DataType::Binary))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let arg = inputs[0].downcast::<Utf8Array>()?;
        let res = arg.encode(self.0.encoder())?;
        Ok(res.into_series())
    }
}

#[must_use]
pub fn encode(input: ExprRef, codec: Codec) -> ExprRef {
    ScalarFunction::new(Encode(codec), vec![input]).into()
}
