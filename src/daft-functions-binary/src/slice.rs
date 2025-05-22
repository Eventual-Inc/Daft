use common_error::{ensure, DaftError, DaftResult};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinarySlice;

#[typetag::serde]
impl ScalarUDF for BinarySlice {
    fn name(&self) -> &'static str {
        "binary_slice"
    }
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 3, SchemaMismatch: "expected 3 inputs");

        let input = inputs.required((0, "input"))?.to_field(schema)?;
        let start = inputs.required((1, "start"))?.to_field(schema)?;
        let length = inputs.required((2, "length"))?.to_field(schema)?;
        match &input.dtype {
            DataType::Binary | DataType::FixedSizeBinary(_) => {
                if start.dtype.is_integer() && (length.dtype.is_integer() || length.dtype.is_null()) {
                    Ok(Field::new(input.name, DataType::Binary))
                } else {
                    Err(DaftError::TypeError(format!(
                        "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                        input.dtype, start.dtype, length.dtype
                    )))
                }
            }
            _ => Err(DaftError::TypeError(format!(
                "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                input.dtype, start.dtype, length.dtype
            ))),
        }
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let data = inputs.required((0, "input"))?;
        let start = inputs.required((1, "start"))?;
        let length = inputs.required((2, "length"))?;
        match data.data_type() {
            DataType::Binary | DataType::FixedSizeBinary(_) => {
                let binary_data = match data.data_type() {
                    DataType::Binary => data.clone(),
                    _ => data.cast(&DataType::Binary)?,
                };
                binary_data.with_binary_array(|arr| {
                    with_match_integer_daft_types!(start.data_type(), |$T| {
                        if length.data_type().is_integer() {
                            with_match_integer_daft_types!(length.data_type(), |$U| {
                                Ok(arr.binary_slice(start.downcast::<<$T as DaftDataType>::ArrayType>()?, Some(length.downcast::<<$U as DaftDataType>::ArrayType>()?))?.into_series())
                            })
                        } else if length.data_type().is_null() {
                            Ok(arr.binary_slice(start.downcast::<<$T as DaftDataType>::ArrayType>()?, None::<&DataArray<Int8Type>>)?.into_series())
                        } else {
                            Err(DaftError::TypeError(format!(
                                "slice not implemented for length type {}",
                                length.data_type()
                            )))
                        }
                    })
                })
            }
            DataType::Null => Ok(data.clone()),
            dt => Err(DaftError::TypeError(format!(
                "Operation not implemented for type {dt}"
            ))),
        }
    }
}
