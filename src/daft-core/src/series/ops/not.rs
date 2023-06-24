use std::ops::Not;

use crate::datatypes::BooleanType;
use crate::series::array_impl::IntoSeries;
use crate::series::Series;
use common_error::DaftResult;

impl Not for &Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        let array = self.downcast::<BooleanType>()?;
        Ok((!array)?.into_series())
    }
}

impl Not for Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        (&self).not()
    }
}
