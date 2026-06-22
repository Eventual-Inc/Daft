use common_error::DaftResult;
use daft_schema::dtype::DataType;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_bool_operator;
use crate::{
    array::ops::DaftLogical,
    datatypes::{DaftArrayType, InferDataType},
    prelude::BooleanArray,
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};

#[derive(Debug, Clone, Copy)]
enum LogicalOp {
    And,
    Or,
    Xor,
}

impl LogicalOp {
    #[cfg(feature = "python")]
    fn py_operator(self) -> &'static str {
        match self {
            Self::And => "and_",
            Self::Or => "or_",
            Self::Xor => "xor",
        }
    }

    fn op_name(self) -> &'static str {
        match self {
            Self::And => "and",
            Self::Or => "or",
            Self::Xor => "xor",
        }
    }

    fn apply<A>(self, lhs: &A, rhs: &A) -> DaftResult<A>
    where
        A: for<'a> DaftLogical<&'a A, Output = DaftResult<A>>,
    {
        match self {
            Self::And => lhs.and(rhs),
            Self::Or => lhs.or(rhs),
            Self::Xor => lhs.xor(rhs),
        }
    }
}

fn logical_op_not_implemented(lhs: &Series, rhs: &Series, op: LogicalOp) -> DaftResult<Series> {
    let left_dtype = lhs.data_type();
    let right_dtype = rhs.data_type();
    let op_name = op.op_name();
    Err(common_error::DaftError::ComputeError(format!(
        "Logical Op: {op_name} not implemented for {left_dtype}, {right_dtype}"
    )))
}

fn cast_downcast_logical_op<A>(
    lhs: &Series,
    rhs: &Series,
    dtype: &DataType,
    op: LogicalOp,
) -> DaftResult<Series>
where
    A: DaftArrayType + IntoSeries + for<'a> DaftLogical<&'a A, Output = DaftResult<A>>,
{
    let lhs = lhs.cast(dtype)?;
    let rhs = rhs.cast(dtype)?;
    let lhs = lhs.downcast::<A>()?;
    let rhs = rhs.downcast::<A>()?;
    Ok(op.apply(lhs, rhs)?.into_series())
}

fn logical_dispatch(lhs: &Series, rhs: &Series, op: LogicalOp) -> DaftResult<Series> {
    let output_type =
        InferDataType::from(lhs.data_type()).logical_op(&InferDataType::from(rhs.data_type()))?;

    match &output_type {
        DataType::Boolean => match (lhs.data_type(), rhs.data_type()) {
            #[cfg(feature = "python")]
            (DataType::Python, _) | (_, DataType::Python) => {
                run_python_binary_bool_operator(lhs, rhs, op.py_operator())
            }
            _ => cast_downcast_logical_op::<BooleanArray>(lhs, rhs, &DataType::Boolean, op),
        },
        output_type if output_type.is_integer() => {
            with_match_integer_daft_types!(output_type, |$T| {
                cast_downcast_logical_op::<<$T as DaftDataType>::ArrayType>(
                    lhs,
                    rhs,
                    output_type,
                    op,
                )
            })
        }
        _ => logical_op_not_implemented(lhs, rhs, op),
    }
}

impl DaftLogical<&Self> for Series {
    type Output = DaftResult<Self>;

    fn and(&self, rhs: &Self) -> Self::Output {
        logical_dispatch(self, rhs, LogicalOp::And)
    }

    fn or(&self, rhs: &Self) -> Self::Output {
        logical_dispatch(self, rhs, LogicalOp::Or)
    }

    fn xor(&self, rhs: &Self) -> Self::Output {
        logical_dispatch(self, rhs, LogicalOp::Xor)
    }
}
