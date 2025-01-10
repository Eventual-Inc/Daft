use std::borrow::Cow;

use common_error::DaftResult;
use daft_schema::prelude::DataType;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_bool_operator;
use crate::{
    array::ops::DaftCompare,
    datatypes::{BooleanArray, InferDataType},
    series::{utils::cast::cast_downcast_op, Series},
    with_match_comparable_daft_types,
};

macro_rules! impl_compare_method {
    ($fname:ident, $pyoperator:expr) => {
        fn $fname(&self, rhs: &Series) -> Self::Output {
            let lhs = self;
            let (output_type, intermediate_type, comparison_type) =
                InferDataType::from(self.data_type())
                    .comparison_op(&InferDataType::from(rhs.data_type()))?;
            assert_eq!(
                output_type,
                DataType::Boolean,
                "All {} Comparisons should result in an Boolean output type, got {output_type}",
                stringify!($fname)
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
                    let output =
                        run_python_binary_bool_operator(&lhs, &rhs, stringify!($pyoperator))?;
                    let bool_array = output
                        .bool()
                        .expect("We expected a Boolean Series from this Python Comparison");
                    Ok(bool_array.clone())
                }
                _ => with_match_comparable_daft_types!(comparison_type, |$T| {
                    cast_downcast_op!(
                        lhs,
                        rhs,
                        &comparison_type,
                        <$T as DaftDataType>::ArrayType,
                        $fname
                    )
                }),
            }
        }
    };
}

impl DaftCompare<&Self> for Series {
    type Output = DaftResult<BooleanArray>;
    impl_compare_method!(equal, eq);
    impl_compare_method!(not_equal, ne);
    impl_compare_method!(lt, lt);
    impl_compare_method!(lte, le);
    impl_compare_method!(gt, gt);
    impl_compare_method!(gte, ge);
    impl_compare_method!(eq_null_safe, eq_null_safe);
}
