use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::pad::{series_pad, PadPlacement};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RPad;

#[typetag::serde]
impl ScalarUDF for RPad {
    fn name(&self) -> &'static str {
        "rpad"
    }
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        ensure!(inputs.len() == 3, SchemaMismatch: "Expected 3 inputs, but received {}", inputs.len());
        let data = inputs.required((0, "input"))?;
        let length = inputs.required((1, "length"))?;
        let pad = inputs.required((2, "pad"))?;

        series_pad(data, length, pad, PadPlacement::Right)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 3, SchemaMismatch: "Expected 3 inputs, but received {}", inputs.len());
        let data = inputs.required((0, "input"))?.to_field(schema)?;
        let length = inputs.required((1, "length"))?.to_field(schema)?;
        let pad = inputs.required((2, "pad"))?.to_field(schema)?;

        ensure!(data.dtype.is_null() || (data.dtype.is_string()
            && length.dtype.is_integer()
            && pad.dtype.is_string()),
            TypeError: "Expects inputs to lpad to be utf8, integer and utf8, but received {}, {}, and {}", data.dtype, length.dtype, pad.dtype
        );
        Ok(data)
    }

    fn docstring(&self) -> &'static str {
        "Pads the string on the right side with the specified string until it reaches the specified length"
    }
}

#[must_use]
pub fn rpad(input: ExprRef, length: ExprRef, pad: ExprRef) -> ExprRef {
    ScalarFunction::new(RPad {}, vec![input, length, pad]).into()
}
