use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::DataType,
    prelude::{Field, IntoSeries, Schema},
    series::Series,
    with_match_integer_daft_types,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::kernels::BinaryArrayExtension;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinarySlice;
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, common_macros::FunctionArgs,
)]
struct BinarySliceArgs<T> {
    input: T,
    start: T,
    #[arg(optional)]
    length: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for BinarySlice {
    fn name(&self) -> &'static str {
        "binary_slice"
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let BinarySliceArgs {
            input,
            start,
            length,
        } = inputs.try_into()?;
        let Field { name, dtype, .. } = input.to_field(schema)?;
        let start = start.to_field(schema)?.dtype;
        let length = length
            .map(|length| length.to_field(schema).map(|f| f.dtype))
            .transpose()?
            .unwrap_or(DataType::Null);

        match &dtype {
            DataType::Binary | DataType::FixedSizeBinary(_) => {
                if start.is_integer() && (length.is_integer() || length.is_null()) {
                    Ok(Field::new(name, DataType::Binary))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                        dtype, start, length
                    )))
                }
            }
            _ => Err(DaftError::TypeError(format!(
                "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                dtype, start, length
            ))),
        }
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let BinarySliceArgs {
            input,
            start,
            length,
        } = inputs.try_into()?;

        match input.data_type() {
            DataType::Binary | DataType::FixedSizeBinary(_) => {
                let binary_data = match input.data_type() {
                    DataType::Binary => input.clone(),
                    _ => input.cast(&DataType::Binary)?,
                };
                binary_data.with_binary_array(|arr| {
                    with_match_integer_daft_types!(start.data_type(), |$T| {
                        match &length {
                            Some(length) if length.data_type().is_integer() => {
                                with_match_integer_daft_types!(length.data_type(), |$U| {
                                    Ok(arr.binary_slice(start.downcast::<<$T as DaftDataType>::ArrayType>()?, Some(length.downcast::<<$U as DaftDataType>::ArrayType>()?))?.into_series())
                                })
                            }
                            Some(length) if length.data_type().is_null() => {
                                Ok(arr.binary_slice(start.downcast::<<$T as DaftDataType>::ArrayType>()?, None::<&DataArray<Int8Type>>)?.into_series())
                            }
                            None => {
                                Ok(arr.binary_slice(start.downcast::<<$T as DaftDataType>::ArrayType>()?, None::<&DataArray<Int8Type>>)?.into_series())
                            }
                            Some(length) => Err(DaftError::TypeError(format!(
                                "slice not implemented for length type {}",
                                length.data_type()
                            ))),
                        }
                    })
                })
            }
            DataType::Null => Ok(input.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }
}
