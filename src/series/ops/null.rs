use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn is_null(&self) -> DaftResult<Series> {
        self.inner.is_null()
    }
}
