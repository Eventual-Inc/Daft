use crate::series::Series;

use common_error::DaftResult;

impl Series {
    pub fn is_null(&self) -> DaftResult<Series> {
        self.inner.is_null()
    }
}
