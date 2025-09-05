use common_error::DaftResult;
use daft_schema::dtype::DataType;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_bool_operator;
use crate::{
    array::ops::DaftLogical,
    datatypes::InferDataType,
    prelude::BooleanArray,
    series::{IntoSeries, Series, utils::cast::cast_downcast_op},
    with_match_integer_daft_types,
};
macro_rules! logical_op_not_implemented {
    ($self:expr, $rhs:expr, $op:ident) => {{
        let left_dtype = $self.data_type();
        let right_dtype = $rhs.data_type();
        let op_name = stringify!($op);
        return Err(common_error::DaftError::ComputeError(format!(
            "Logical Op: {op_name} not implemented for {left_dtype}, {right_dtype}"
        )));
    }};
}

impl DaftLogical<&Self> for Series {
    type Output = DaftResult<Self>;

    fn and(&self, rhs: &Self) -> Self::Output {
        let lhs = self;
        let output_type = InferDataType::from(lhs.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => match (lhs.data_type(), rhs.data_type()) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => {
                    run_python_binary_bool_operator(lhs, rhs, "and_")
                }
                _ => Ok(
                    cast_downcast_op!(lhs, rhs, &DataType::Boolean, BooleanArray, and)?
                        .into_series(),
                ),
            },
            output_type if output_type.is_integer() => {
                with_match_integer_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(
                        self,
                        rhs,
                        output_type,
                        <$T as DaftDataType>::ArrayType,
                        and
                    )?.into_series())
                })
            }

            _ => logical_op_not_implemented!(self, rhs, and),
        }
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        let lhs = self;
        let output_type = InferDataType::from(self.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => match (lhs.data_type(), rhs.data_type()) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => {
                    run_python_binary_bool_operator(lhs, rhs, "or_")
                }
                _ => Ok(
                    cast_downcast_op!(lhs, rhs, &DataType::Boolean, BooleanArray, or)?
                        .into_series(),
                ),
            },
            output_type if output_type.is_integer() => {
                with_match_integer_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(
                        self,
                        rhs,
                        output_type,
                        <$T as DaftDataType>::ArrayType,
                        or
                    )?.into_series())
                })
            }
            _ => logical_op_not_implemented!(self, rhs, or),
        }
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        let lhs = self;
        let output_type = InferDataType::from(self.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => match (lhs.data_type(), rhs.data_type()) {
                #[cfg(feature = "python")]
                (DataType::Python, _) | (_, DataType::Python) => {
                    run_python_binary_bool_operator(lhs, rhs, "xor")
                }
                _ => Ok(
                    cast_downcast_op!(lhs, rhs, &DataType::Boolean, BooleanArray, xor)?
                        .into_series(),
                ),
            },
            output_type if output_type.is_integer() => {
                with_match_integer_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(
                        self,
                        rhs,
                        output_type,
                        <$T as DaftDataType>::ArrayType,
                        xor
                    )?.into_series())
                })
            }
            _ => logical_op_not_implemented!(self, rhs, xor),
        }
    }
}
