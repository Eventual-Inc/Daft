use super::match_types_on_series;
use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
        let (if_true, if_false) = match_types_on_series(self, other)?;
        if_true.inner.if_else(&if_false, predicate)
    }
}
