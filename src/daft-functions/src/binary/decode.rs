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

use super::codecs::Codec;
use crate::invalid_argument_err;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Decode(Codec);

#[typetag::serde]
impl ScalarUDF for Decode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str {
        "decode"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
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
        Ok(Field::new(arg.name, DataType::Binary))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs[0].data_type() {
            DataType::Binary => {
                let arg = inputs[0].downcast::<BinaryArray>()?;
                let res = arg.decode(self.0.decoder())?;
                Ok(res.into_series())
            }
            DataType::FixedSizeBinary(_) => {
                let arg = inputs[0].downcast::<FixedSizeBinaryArray>()?;
                let res = arg.decode(self.0.decoder())?;
                Ok(res.into_series())
            }
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}

#[must_use]
pub fn decode(input: ExprRef, codec: Codec) -> ExprRef {
    ScalarFunction::new(Decode(codec), vec![input]).into()
}
