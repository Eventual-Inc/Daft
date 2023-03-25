use std::ops::Not;

use crate::array::BaseArray;
use crate::datatypes::BooleanType;
use crate::error::DaftResult;
use crate::series::Series;

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
