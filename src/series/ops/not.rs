use std::ops::Not;

use crate::datatypes::BooleanType;
use crate::error::DaftResult;
use crate::series::Series;

impl Not for &Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        let _array = self.downcast::<BooleanType>()?;
        todo!()
        // Ok(array.not()?.into_series())
    }
}

impl Not for Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        (&self).not()
    }
}
