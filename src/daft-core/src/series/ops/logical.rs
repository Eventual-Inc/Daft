use common_error::DaftResult;
use daft_schema::dtype::DataType;

use crate::{
    array::ops::DaftLogical,
    datatypes::InferDataType,
    series::{utils::cast::cast_downcast_op, IntoSeries, Series},
    with_match_integer_daft_types,
};

macro_rules! binary_op_not_implemented {
    ($self:expr, $rhs:expr, $op:ident) => {{
        let left_dtype = $self.data_type();
        let right_dtype = $rhs.data_type();
        let op_name = stringify!($op);
        return Err(common_error::DaftError::ComputeError(format!(
            "Binary Op: {op_name} not implemented for {left_dtype}, {right_dtype}"
        )));
    }};
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<Series>;

    fn and(&self, rhs: &Series) -> Self::Output {
        let output_type = InferDataType::from(self.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            DataType::Python => {
                todo!("python happy path")
            }
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
            _ => binary_op_not_implemented!(self, rhs, and),
        }
    }

    fn or(&self, rhs: &Series) -> Self::Output {
        let output_type = InferDataType::from(self.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            DataType::Python => {
                todo!("python happy path")
            }
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
            _ => binary_op_not_implemented!(self, rhs, or),
        }
    }

    fn xor(&self, rhs: &Series) -> Self::Output {
        let output_type = InferDataType::from(self.data_type())
            .logical_op(&InferDataType::from(rhs.data_type()))?;
        match &output_type {
            DataType::Boolean => {
                todo!("boolean happy path")
            }
            #[cfg(feature = "python")]
            DataType::Python => {
                todo!("python happy path")
            }
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
            _ => binary_op_not_implemented!(self, rhs, xor),
        }
    }
}
