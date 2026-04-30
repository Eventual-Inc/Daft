use std::borrow::Cow;

use daft_common::error::DaftResult;
use daft_schema::prelude::DataType;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_bool_operator;
use crate::{
    array::ops::DaftCompare,
    datatypes::{BooleanArray, InferDataType},
    series::Series,
    with_match_comparable_daft_types,
};

#[derive(Debug, Clone, Copy)]
enum CompareOp {
    Equal,
    NotEqual,
    Lt,
    Lte,
    Gt,
    Gte,
    EqNullSafe,
}

impl CompareOp {
    #[cfg(feature = "python")]
    fn py_operator(self) -> &'static str {
        match self {
            Self::Equal => "eq",
            Self::NotEqual => "ne",
            Self::Lt => "lt",
            Self::Lte => "le",
            Self::Gt => "gt",
            Self::Gte => "ge",
            Self::EqNullSafe => "eq_null_safe",
        }
    }

    fn apply<L, R>(self, lhs: &L, rhs: &R) -> DaftResult<BooleanArray>
    where
        L: for<'a> DaftCompare<&'a R, Output = DaftResult<BooleanArray>>,
    {
        match self {
            Self::Equal => lhs.equal(rhs),
            Self::NotEqual => lhs.not_equal(rhs),
            Self::Lt => lhs.lt(rhs),
            Self::Lte => lhs.lte(rhs),
            Self::Gt => lhs.gt(rhs),
            Self::Gte => lhs.gte(rhs),
            Self::EqNullSafe => lhs.eq_null_safe(rhs),
        }
    }
}

fn compare_dispatch(lhs: &Series, rhs: &Series, op: CompareOp) -> DaftResult<BooleanArray> {
    let (output_type, intermediate_type, comparison_type) = InferDataType::from(lhs.data_type())
        .comparison_op(&InferDataType::from(rhs.data_type()))?;
    assert_eq!(
        output_type,
        DataType::Boolean,
        "All comparisons should result in a Boolean output type, got {output_type}",
    );
    let (lhs, rhs) = if let Some(intermediate_type) = intermediate_type {
        (
            Cow::Owned(lhs.cast(&intermediate_type)?),
            Cow::Owned(rhs.cast(&intermediate_type)?),
        )
    } else {
        (Cow::Borrowed(lhs), Cow::Borrowed(rhs))
    };

    match comparison_type {
        #[cfg(feature = "python")]
        DataType::Python => {
            let output = run_python_binary_bool_operator(&lhs, &rhs, op.py_operator())?;
            let bool_array = output
                .bool()
                .expect("We expected a Boolean Series from this Python Comparison");
            Ok(bool_array.clone())
        }
        DataType::List(_) => {
            let lhs = lhs.cast(&comparison_type)?;
            let rhs = rhs.cast(&comparison_type)?;
            let lhs = lhs.list()?;
            let rhs = rhs.list()?;
            op.apply(lhs, rhs)
        }
        DataType::FixedSizeList(_, _) => {
            let lhs = lhs.cast(&comparison_type)?;
            let rhs = rhs.cast(&comparison_type)?;
            let lhs = lhs.fixed_size_list()?;
            let rhs = rhs.fixed_size_list()?;
            op.apply(lhs, rhs)
        }
        DataType::Struct(_) => {
            let lhs = lhs.cast(&comparison_type)?;
            let rhs = rhs.cast(&comparison_type)?;
            let lhs = lhs.struct_()?;
            let rhs = rhs.struct_()?;
            op.apply(lhs, rhs)
        }
        _ => with_match_comparable_daft_types!(comparison_type, |$T| {
            let lhs = lhs.cast(&comparison_type)?;
            let rhs = rhs.cast(&comparison_type)?;
            let lhs = lhs.downcast::<<$T as DaftDataType>::ArrayType>()?;
            let rhs = rhs.downcast::<<$T as DaftDataType>::ArrayType>()?;
            op.apply(lhs, rhs)
        }),
    }
}

impl DaftCompare<&Self> for Series {
    type Output = DaftResult<BooleanArray>;

    fn equal(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::Equal)
    }

    fn not_equal(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::NotEqual)
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::Lt)
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::Lte)
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::Gt)
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::Gte)
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        compare_dispatch(self, rhs, CompareOp::EqNullSafe)
    }
}
