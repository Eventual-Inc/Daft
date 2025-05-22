use common_error::{ensure, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{BinaryArray, FixedSizeBinaryArray, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::codecs::Codec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryEncode;

#[typetag::serde]
impl ScalarUDF for BinaryEncode {
    fn name(&self) -> &'static str {
        "binary_encode"
    }
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 arguments, found {}",
            inputs.len()
        );
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let _: Codec = inputs.extract((1, "codec"))?;
        ensure!(
            matches!(
                input.dtype,
                DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
            ),
            TypeError: "Expected argument to be Utf8, Binary or FixedSizeBinary, but recei2ved {}",
            input.dtype
        );

        Ok(Field::new(input.name, DataType::Binary))
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let codec: Codec = inputs.extract((1, "codec"))?;
        match input.data_type() {
            DataType::Binary => input
                .downcast::<BinaryArray>()?
                .transform(codec.encoder())
                .map(IntoSeries::into_series),
            DataType::FixedSizeBinary(_) => input
                .downcast::<FixedSizeBinaryArray>()?
                .transform(codec.encoder())
                .map(IntoSeries::into_series),
            DataType::Utf8 if codec == Codec::Utf8 => input.cast(&DataType::Binary),
            DataType::Utf8 => input
                .downcast::<Utf8Array>()?
                .encode(codec.encoder())
                .map(IntoSeries::into_series),
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryTryEncode;

#[typetag::serde]
impl ScalarUDF for BinaryTryEncode {
    fn name(&self) -> &'static str {
        "binary_try_encode"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            inputs.len() == 2,
            SchemaMismatch: "Expected 2 arguments, found {}",
            inputs.len()
        );
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let _: Codec = inputs.extract((1, "codec"))?;
        ensure!(
            matches!(
                input.dtype,
                DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
            ),
            TypeError: "Expected argument to be Utf8, Binary or FixedSizeBinary, but received {}",
            input.dtype
        );

        Ok(Field::new(input.name, DataType::Binary))
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let codec: Codec = inputs.extract((1, "codec"))?;
        match input.data_type() {
            DataType::Binary => input
                .downcast::<BinaryArray>()?
                .try_transform(codec.encoder())
                .map(IntoSeries::into_series),
            DataType::FixedSizeBinary(_) => input
                .downcast::<FixedSizeBinaryArray>()?
                .try_transform(codec.encoder())
                .map(IntoSeries::into_series),
            DataType::Utf8 if codec == Codec::Utf8 => input.cast(&DataType::Binary),
            DataType::Utf8 => input
                .downcast::<Utf8Array>()?
                .try_encode(codec.encoder())
                .map(IntoSeries::into_series),
            _ => unreachable!("type checking handled in to_field"),
        }
    }
}
