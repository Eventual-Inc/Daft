use common_error::DaftResult;

use crate::{IntoSeries, Series};

impl Series {
    pub fn hll(&self) -> DaftResult<Self> {
        let series = self.hash(None)?.into_series();
        Ok(series)
    }
}
