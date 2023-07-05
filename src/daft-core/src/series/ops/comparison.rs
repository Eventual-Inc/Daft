use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::{BooleanArray, BooleanType, DataType},
    series::Series,
    with_match_comparable_daft_types,
};
use common_error::{DaftError, DaftResult};

use super::match_types_on_series;
#[cfg(feature = "python")]
use super::py_binary_op_utilfn;

macro_rules! py_binary_op_bool {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics_bool")
    };
}

macro_rules! impl_compare {
    ($fname:ident, $pyoperator:expr) => {
        fn $fname(&self, rhs: &Series) -> Self::Output {
            let (lhs, rhs) = match_types_on_series(self, rhs)?;

            #[cfg(feature = "python")]
            if lhs.data_type() == &DataType::Python {
                return py_binary_op_bool!(lhs, rhs, $pyoperator)
                    .downcast::<BooleanType>()
                    .cloned();
            }

            let lhs = lhs.as_physical()?;
            let rhs = rhs.as_physical()?;

            with_match_comparable_daft_types!(lhs.data_type(), |$T| {
                let lhs = lhs.downcast::<$T>()?;
                let rhs = rhs.downcast::<$T>()?;
                lhs.$fname(rhs)
            })
        }
    };
}

impl DaftCompare<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    impl_compare!(equal, "eq");
    impl_compare!(not_equal, "ne");
    impl_compare!(lt, "lt");
    impl_compare!(lte, "le");
    impl_compare!(gt, "gt");
    impl_compare!(gte, "ge");
}

macro_rules! impl_logical {
    ($fname:ident) => {
        fn $fname(&self, other: &Series) -> Self::Output {
            self.inner.$fname(other)
        }
    };
}

impl DaftLogical<&Series> for Series {
    type Output = DaftResult<BooleanArray>;

    impl_logical!(and);
    impl_logical!(or);
    impl_logical!(xor);
}
