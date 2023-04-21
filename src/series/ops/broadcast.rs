use crate::{error::DaftResult, series::Series, with_match_physical_daft_types};

use crate::array::ops::broadcast::Broadcastable;
use crate::array::BaseArray;

impl Series {
    pub fn broadcast(&self, num: usize) -> DaftResult<Series> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Series named: {}",
                self.name()
            )));
        }

        let s = self.as_physical()?;
        let result = with_match_physical_daft_types!(self.data_type(), |$T| {
            let array = s.downcast::<$T>()?;
            array.broadcast(num)?.into_series()
        });

        if result.data_type() != self.data_type() {
            return result.cast(self.data_type());
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::BaseArray,
        datatypes::{DataType, Int64Array, Utf8Array},
        error::DaftResult,
    };

    #[test]
    fn broadcast_int() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1])).into_series();
        let a = a.broadcast(10)?;
        assert_eq!(a.len(), 10);
        assert_eq!(*a.data_type(), DataType::Int64);

        let array = a.i64()?;
        for i in 0..a.len() {
            assert_eq!(array.get(i).unwrap(), 1);
        }

        Ok(())
    }

    #[test]
    fn broadcast_int_null() -> DaftResult<()> {
        let a = Int64Array::full_null("a", &DataType::Int64, 1).into_series();
        let a = a.broadcast(10)?;
        assert_eq!(a.len(), 10);
        assert_eq!(*a.data_type(), DataType::Int64);

        let array = a.i64()?;
        for i in 0..a.len() {
            assert!(array.get(i).is_none());
        }

        Ok(())
    }

    #[test]
    fn broadcast_utf8() -> DaftResult<()> {
        let a = Utf8Array::from(("a", vec!["1"].as_slice())).into_series();
        let a = a.broadcast(10)?;
        assert_eq!(a.len(), 10);
        assert_eq!(*a.data_type(), DataType::Utf8);

        let array = a.utf8()?;
        for i in 0..a.len() {
            assert_eq!(array.get(i).unwrap(), "1");
        }

        Ok(())
    }

    #[test]
    fn broadcast_utf8_null() -> DaftResult<()> {
        let a = Utf8Array::full_null("a", &DataType::Utf8, 1).into_series();
        let a = a.broadcast(10)?;
        assert_eq!(a.len(), 10);
        assert_eq!(*a.data_type(), DataType::Utf8);

        let array = a.utf8()?;
        for i in 0..a.len() {
            assert!(array.get(i).is_none());
        }

        Ok(())
    }
}
