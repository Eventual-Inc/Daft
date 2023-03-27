use super::match_types_on_series;
use crate::{
    array::BaseArray, datatypes, error::DaftResult, series::Series, with_match_physical_daft_types,
};

impl Series {
    pub fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
        let (if_true, if_false) = match_types_on_series(self, other)?;
        with_match_physical_daft_types!(if_true.data_type(), |$T| {
            let if_true_array = if_true.downcast::<$T>()?;
            let if_false_array = if_false.downcast::<$T>()?;
            let predicate_array = predicate.downcast::<datatypes::BooleanType>()?;
            Ok(if_true_array.if_else(if_false_array, predicate_array)?.into_series())
        })
    }
}
