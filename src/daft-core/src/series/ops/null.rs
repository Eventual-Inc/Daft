use common_error::DaftResult;

use crate::series::Series;

impl Series {
    pub fn is_null(&self) -> DaftResult<Self> {
        self.inner.is_null()
    }

    pub fn not_null(&self) -> DaftResult<Self> {
        self.inner.not_null()
    }

    pub fn fill_null(&self, fill_value: &Self) -> DaftResult<Self> {
        let predicate = self.not_null()?;
        self.if_else(fill_value, &predicate)
    }
}
