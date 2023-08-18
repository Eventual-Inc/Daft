use crate::series::Series;

use common_error::DaftResult;
impl Series {
    pub fn broadcast(&self, num: usize) -> DaftResult<Series> {
        self.inner.broadcast(num)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::ops::full::FullNull;
    use crate::datatypes::{DataType, Int64Array, Utf8Array};
    use crate::series::array_impl::IntoSeries;
    use common_error::DaftResult;

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
