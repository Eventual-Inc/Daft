use crate::series::Series;

use common_error::DaftResult;

impl Series {
    pub fn is_null(&self) -> DaftResult<Series> {
        self.inner.is_null()
    }

    pub fn not_null(&self) -> DaftResult<Series> {
        self.inner.not_null()
    }

    pub fn fill_null(&self, fill_value: &Series) -> DaftResult<Series> {
        let predicate = self.not_null()?;
        self.if_else(fill_value, &predicate)
    }
}
