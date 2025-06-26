use common_error::DaftResult;

use crate::series::Series;

impl Series {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.inner.size_bytes()
    }
}
