use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, unary_utf8_evaluate, unary_utf8_to_field};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UrlEncode;

#[typetag::serde]
impl ScalarUDF for UrlEncode {
    fn name(&self) -> &'static str {
        "url_encode"
    }
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                Ok(arr
                    .unary_broadcasted_op(|val| utf8_percent_encode(val, NON_ALPHANUMERIC).to_string().into())?
                    .into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "URL-encodes a UTF-8 string."
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UrlDecode;

#[typetag::serde]
impl ScalarUDF for UrlDecode {
    fn name(&self) -> &'static str {
        "url_decode"
    }
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        unary_utf8_evaluate(inputs, |s| {
            s.with_utf8_array(|arr| {
                // percent_decode_str can return an error if the string is not valid utf-8 after decoding,
                // but since percent_decode_str().decode_utf8_lossy() replaces invalid sequences,
                // we'll just use decode_utf8_lossy().
                Ok(arr
                    .unary_broadcasted_op(|val| percent_decode_str(val).decode_utf8_lossy().into_owned().into())?
                    .into_series())
            })
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        unary_utf8_to_field(inputs, schema, self.name(), DataType::Utf8)
    }

    fn docstring(&self) -> &'static str {
        "URL-decodes a UTF-8 string."
    }
}

#[must_use]
pub fn url_encode(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(UrlEncode {}, vec![input]).into()
}

#[must_use]
pub fn url_decode(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(UrlDecode {}, vec![input]).into()
}
