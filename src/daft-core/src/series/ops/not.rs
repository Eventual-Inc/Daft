use std::ops::Not;

use daft_common_error::DaftResult;

use crate::{
    datatypes::{BooleanArray, DataType},
    series::{Series, array_impl::IntoSeries},
};

impl Not for &Series {
    type Output = DaftResult<Series>;
    fn not(self) -> Self::Output {
        if *self.data_type() == DataType::Null {
            return Ok(Series::full_null(
                self.name(),
                &DataType::Boolean,
                self.len(),
            ));
        }
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

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use crate::{
        array::ops::full::FullNull, datatypes::DataType, prelude::NullArray,
        series::array_impl::IntoSeries,
    };

    #[test]
    fn test_not_boolean() {
        let arr = crate::datatypes::BooleanArray::from_slice("x", &[true, false, true]);
        let s = arr.into_series();
        let result = s.not().unwrap();
        let bools: Vec<_> = result.bool().unwrap().into_iter().collect();
        assert_eq!(bools, vec![Some(false), Some(true), Some(false)]);
    }

    #[test]
    fn test_not_null_series_returns_boolean_null() {
        // NOT on a Null-typed series must return a Boolean null series (SQL: NOT NULL = NULL).
        let null_series = NullArray::full_null("x", &DataType::Null, 3).into_series();
        let result = null_series.not().unwrap();
        assert_eq!(*result.data_type(), DataType::Boolean);
        assert_eq!(result.len(), 3);
        // Every value should be null.
        let bools: Vec<_> = result.bool().unwrap().into_iter().collect();
        assert!(bools.iter().all(|v| v.is_none()));
    }
}
