use common_error::{DaftResult, ensure};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{BinaryArray, FixedSizeBinaryArray, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
use serde::{Deserialize, Serialize};

use super::codecs::Codec;
use crate::kernels::BinaryArrayExtension;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryEncode;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, FunctionArgs)]
struct Args<T> {
    input: T,
    codec: Codec,
}

#[typetag::serde]
impl ScalarUDF for BinaryEncode {
    fn name(&self) -> &'static str {
        "encode"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, codec } = inputs.try_into()?;
        let input = input.to_field(schema)?;

        ensure!(
            matches!(
                input.dtype,
                DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)
            ),
            TypeError: "Expected argument to be Utf8, Binary or FixedSizeBinary, but recei2ved {}",
            input.dtype
        );

        Ok(Field::new(input.name, codec.returns()))
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, codec } = inputs.try_into()?;

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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryTryEncode;

#[typetag::serde]
impl ScalarUDF for BinaryTryEncode {
    fn name(&self) -> &'static str {
        "try_encode"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, codec: _ } = inputs.try_into()?;
        let input = input.to_field(schema)?;
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

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, codec } = inputs.try_into()?;

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
