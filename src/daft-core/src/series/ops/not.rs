use std::ops::Not;

use common_error::DaftResult;

use crate::{
    datatypes::BooleanArray,
    series::{Series, array_impl::IntoSeries},
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
