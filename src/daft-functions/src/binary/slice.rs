use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::DataType,
    prelude::{Field, IntoSeries, Schema},
    series::Series,
    with_match_integer_daft_types,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinarySlice {}

#[typetag::serde]
impl ScalarUDF for BinarySlice {
    fn name(&self) -> &'static str {
        "binary_slice"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, start, length] => {
                let data = data.to_field(schema)?;
                let start = start.to_field(schema)?;
                let length = length.to_field(schema)?;

                match &data.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(_) => {
                        if start.dtype.is_integer() && (length.dtype.is_integer() || length.dtype.is_null()) {
                            Ok(Field::new(data.name, DataType::Binary))
                        } else {
                            Err(DaftError::TypeError(format!(
                                "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                                data.dtype, start.dtype, length.dtype
                            )))
                        }
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expects inputs to binary_slice to be binary, integer and integer or null but received {}, {} and {}",
                        data.dtype, start.dtype, length.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input args, got {}",
                inputs.len()
            ))),
        }
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        self.evaluate_from_series(&inputs)
    }
    fn evaluate_from_series(&self, inputs: &[Series]) -> DaftResult<Series> {
        let data = &inputs[0];
        let start = &inputs[1];
        let length = &inputs[2];

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

#[must_use]
pub fn binary_slice(input: ExprRef, start: ExprRef, length: ExprRef) -> ExprRef {
    ScalarFunction::new(BinarySlice {}, vec![input, start, length]).into()
}
