use crate::series::Series;

use common_error::DaftResult;

impl Series {
    pub fn head(&self, num: usize) -> DaftResult<Series> {
        if num >= self.len() {
            return Ok(self.clone());
        }
        self.inner.head(num)
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
        self.inner.slice(start, end)
    }

    pub fn take(&self, idx: &Series) -> DaftResult<Series> {
        self.inner.take(idx)
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        self.inner.str_value(idx)
    }

    pub fn html_value(&self, idx: usize) -> String {
        self.inner.html_value(idx)
    }
}
