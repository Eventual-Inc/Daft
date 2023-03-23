use crate::{
    array::BaseArray, datatypes, error::DaftResult, series::Series,
    with_match_numeric_and_utf_daft_types,
};

impl Series {
    pub fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
        // TODO: [RUST-INT] implement for more types
        with_match_numeric_and_utf_daft_types!(self.data_type(), |$T| {
            let self_array = self.downcast::<$T>()?;
            let other_array = other.downcast::<$T>()?;
            let predicate_array = predicate.downcast::<datatypes::BooleanType>()?;
            Ok(self_array.if_else(other_array, predicate_array)?.into_series())
        })
    }
}
