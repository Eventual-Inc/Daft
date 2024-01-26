use crate::{datatypes::Utf8Array, series::Series, IntoSeries};

use arrow2::types::IndexRange;
use common_error::DaftResult;

impl Series {
    pub fn head(&self, num: usize) -> DaftResult<Series> {
        if num >= self.len() {
            return Ok(self.clone());
        }
        self.inner.head(num)
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
        let l = self.len();
        self.inner.slice(start.min(l), end.min(l))
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

    pub fn to_str_values(&self) -> DaftResult<Self> {
        let iter =
            IndexRange::new(0i64, self.len() as i64).map(|i| self.str_value(i as usize).ok());
        let array = Utf8Array::from_iter(self.name(), iter);
        Ok(array.into_series())
    }
}
