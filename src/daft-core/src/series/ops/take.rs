use crate::{
    datatypes::Utf8Array,
    series::{IntoSeries, Series},
};

use arrow2::types::IndexRange;
use common_display::table_display::StrValue;
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

    pub fn html_value(&self, idx: usize) -> String {
        self.inner.html_value(idx)
    }

    pub fn to_str_values(&self) -> DaftResult<Self> {
        let iter =
            IndexRange::new(0i64, self.len() as i64).map(|i| Some(self.str_value(i as usize)));
        let array = Utf8Array::from_iter(self.name(), iter);
        Ok(array.into_series())
    }
}

impl StrValue for Series {
    fn str_value(&self, idx: usize) -> String {
        self.inner.str_value(idx).unwrap()
    }
}
