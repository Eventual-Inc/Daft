use std::borrow::Cow;

use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::{BooleanArray, InferDataType},
    series::Series, with_match_comparable_daft_types,
};
use arrow2::compute::comparison;
use common_error::DaftResult;
use daft_schema::dtype::DataType;


#[cfg(feature = "python")]
use crate::{datatypes::PythonArray, series::ops::py_binary_op_utilfn};

#[cfg(feature = "python")]
macro_rules! py_binary_op {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {

        // this can just be a function... no need for macro
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics")
    };
}
#[cfg(feature = "python")]
macro_rules! py_binary_op_bool {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {

        // this can be a function
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics_bool")
    };
}

macro_rules! impl_compare_method {
    ($fname:ident, $pyoperator:expr) => {
        fn $fname(&self, rhs: &Series) -> Self::Output {
            let lhs = self;
            let (output_type, intermediate_type, comparison_type) = InferDataType::from(self.data_type()).comparison_op(&InferDataType::from(rhs.data_type()))?;
            assert_eq!(output_type, DataType::Boolean, "All {} Comparisons should result in an Boolean output type, got {output_type}", stringify!($fname));
            let (lhs, rhs) = if let Some(intermediate_type) = intermediate_type {
                (Cow::Owned(lhs.cast(&intermediate_type)?), Cow::Owned(rhs.cast(&intermediate_type)?))
            } else {
                (Cow::Borrowed(lhs), Cow::Borrowed(rhs))
            };
            match comparison_type {
                DataType::Python => {
                    let output = py_binary_op_bool!(lhs, rhs, stringify!($pyoperator));
                    let bool_array = output.bool().expect("We expected a Boolean Series from this Python Comparison");
                    Ok(bool_array.clone())
                },
                _ => with_match_comparable_daft_types!(comparison_type, |$T| {
                    cast_downcast_op!(lhs, rhs, &comparison_type, <$T as DaftDataType>::ArrayType, $fname)
                }),
            }
            }
    };
}

macro_rules! cast_downcast_op {
    ($lhs:expr, $rhs:expr, $ty_expr:expr, $ty_type:ty, $op:ident) => {{
        let lhs = $lhs.cast($ty_expr)?;
        let rhs = $rhs.cast($ty_expr)?;
        let lhs = lhs.downcast::<$ty_type>()?;
        let rhs = rhs.downcast::<$ty_type>()?;
        lhs.$op(rhs)
    }};
}




impl DaftCompare<&Series> for Series {
    type Output = DaftResult<BooleanArray>;
    impl_compare_method!(equal, eq);
    impl_compare_method!(not_equal, ne);
    impl_compare_method!(lt, lt);
    impl_compare_method!(lte, le);
    impl_compare_method!(gt, gt);
    impl_compare_method!(gte, ge);
}


macro_rules! binary_op_not_implemented {
    ($self:expr, $rhs:expr, $op:ident) => {{
        let left_dtype = $self.data_type();
        let right_dtype = $rhs.data_type();
        let op_name = stringify!($op);
        return Err(common_error::DaftError::ComputeError(format!("Binary Op: {op_name} not implemented for {left_dtype}, {right_dtype}")));
    }};
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<Series>;

    fn and(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            },
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, and)
        }
    }
    
    fn or(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            },
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, or)
        }
    }

    fn xor(&self, rhs: &Series) -> Self::Output {
        match (self.data_type(), rhs.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                todo!("boolean happy path")
            },
            (DataType::Python, DataType::Python) => {
                todo!("python happy path")
            }
            (DataType::Python, DataType::Boolean) | (DataType::Boolean, DataType::Python) => {
                todo!("cast to python then happy path")
            }
            _ => binary_op_not_implemented!(self, rhs, xor)
        }
    }

}

