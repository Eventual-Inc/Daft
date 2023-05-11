use crate::{error::DaftResult, series::Series};

impl Series {
    pub fn head(&self, num: usize) -> DaftResult<Series> {
        if num >= self.len() {
            return Ok(self.clone());
        }
        self.inner.head(num)

        // let s = self.as_physical()?;

        // let result = with_match_physical_daft_types!(s.data_type(), |$T| {
        //     s.downcast::<$T>()?.head(num)?.into_series()
        // });
        // if result.data_type() != self.data_type() {
        //     return result.cast(self.data_type());
        // }
        // Ok(result)
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Series> {
        self.inner.slice(start, end)

        // let s = self.as_physical()?;

        // let result = with_match_physical_daft_types!(s.data_type(), |$T| {
        //     s.downcast::<$T>()?.slice(start, end)?.into_series()
        // });
        // if result.data_type() != self.data_type() {
        //     return result.cast(self.data_type());
        // }
        // Ok(result)
    }

    pub fn take(&self, idx: &Series) -> DaftResult<Series> {
        self.inner.take(idx)
        // let s = self.as_physical()?;
        // let result = with_match_physical_daft_types!(s.data_type(), |$T| {
        //     with_match_integer_daft_types!(idx.data_type(), |$S| {
        //         s.downcast::<$T>()?.take(idx.downcast::<$S>()?)?.into_series()
        //     })
        // });
        // if result.data_type() != self.data_type() {
        //     return result.cast(self.data_type());
        // }
        // Ok(result)
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        self.inner.str_value(idx)
        // let s = self.as_physical()?;
        // with_match_physical_daft_types!(s.data_type(), |$T| {
        //     s.downcast::<$T>()?.str_value(idx)
        // })
    }
}
