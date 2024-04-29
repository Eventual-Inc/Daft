use super::match_types_on_series;
use crate::series::Series;

use common_error::DaftResult;

impl Series {
    pub fn if_else(&self, other: &Series, predicate: &Series) -> DaftResult<Series> {
        let series = match_types_on_series(vec![self, other])?;
        let if_true = &series[0];
        let if_false = &series[1];

        if_true.inner.if_else(if_false, predicate)
    }
}
