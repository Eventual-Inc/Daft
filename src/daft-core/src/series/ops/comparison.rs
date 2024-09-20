use common_error::DaftResult;

use crate::{
    array::ops::{DaftCompare, DaftLogical},
    datatypes::BooleanArray,
    series::Series,
};

macro_rules! call_inner {
    ($fname:ident) => {
        fn $fname(&self, other: &Series) -> Self::Output {
            self.inner.$fname(other)
        }
    };
}

impl DaftCompare<&Self> for Series {
    type Output = DaftResult<BooleanArray>;

    call_inner!(equal);
    call_inner!(not_equal);
    call_inner!(lt);
    call_inner!(lte);
    call_inner!(gt);
    call_inner!(gte);
}

impl DaftLogical<&Self> for Series {
    type Output = DaftResult<Self>;

    call_inner!(and);
    call_inner!(or);
    call_inner!(xor);
}
