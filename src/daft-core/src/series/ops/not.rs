use std::ops::Not;

use common_error::DaftResult;

use crate::{
    datatypes::BooleanArray,
    series::{array_impl::IntoSeries, Series},
};

impl Not for &Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        let array = self.downcast::<BooleanArray>()?;
        Ok((!array)?.into_series())
    }
}

impl Not for Series {
    type Output = DaftResult<Self>;
    fn not(self) -> Self::Output {
        (&self).not()
    }
}
