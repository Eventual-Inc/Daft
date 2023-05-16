use crate::error::DaftResult;
use crate::series::Series;

impl Series {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.inner.size_bytes()
    }
}
