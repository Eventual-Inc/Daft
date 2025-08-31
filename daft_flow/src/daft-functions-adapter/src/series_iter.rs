use std::iter::Iterator;

use daft_core::{
    datatypes::{DaftDataType, DaftPhysicalType, DaftPrimitiveType},
    series::Series,
};

use crate::error::{FunctionsAdapterError, FunctionsAdapterResult};

/// Iterator over string values in a Series
pub struct StringSeriesIter<'a> {
    array: &'a daft_core::datatypes::Utf8Array,
    index: usize,
}

impl<'a> StringSeriesIter<'a> {
    pub fn new(series: &'a Series) -> FunctionsAdapterResult<Self> {
        // Check if the series data type matches the expected type
        let expected_dtype = daft_core::datatypes::Utf8Type::get_dtype();
        if series.data_type() != &expected_dtype {
            return Err(FunctionsAdapterError::TypeMismatch {
                expected: format!("{:?}", expected_dtype),
                actual: format!("{:?}", series.data_type()),
            });
        }

        let array = series
            .utf8()
            .map_err(|_| FunctionsAdapterError::IteratorError {
                message: "Failed to downcast Series to Utf8Array in StringSeriesIter".to_string(),
            })?;

        Ok(Self { array, index: 0 })
    }
}

impl<'a> Iterator for StringSeriesIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            let result = self.array.get(self.index);
            self.index += 1;
            Some(result)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.array.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

/// Iterator over binary values in a Series
pub struct BinarySeriesIter<'a> {
    array: &'a daft_core::datatypes::BinaryArray,
    index: usize,
}

impl<'a> BinarySeriesIter<'a> {
    pub fn new(series: &'a Series) -> FunctionsAdapterResult<Self> {
        // Check if the series data type matches the expected type
        let expected_dtype = daft_core::datatypes::BinaryType::get_dtype();
        if series.data_type() != &expected_dtype {
            return Err(FunctionsAdapterError::TypeMismatch {
                expected: format!("{:?}", expected_dtype),
                actual: format!("{:?}", series.data_type()),
            });
        }

        let array = series
            .binary()
            .map_err(|_| FunctionsAdapterError::IteratorError {
                message: "Failed to downcast Series to BinaryArray in BinarySeriesIter".to_string(),
            })?;

        Ok(Self { array, index: 0 })
    }
}

impl<'a> Iterator for BinarySeriesIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            let result = self.array.get(self.index);
            self.index += 1;
            Some(result)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.array.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

/// Generic iterator over primitive numeric values in a Series
pub struct PrimitiveSeriesIter<'a, T> {
    array: &'a daft_core::datatypes::DataArray<T>,
    index: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> PrimitiveSeriesIter<'a, T>
where
    T: DaftPrimitiveType + DaftPhysicalType,
{
    pub fn new(series: &'a Series) -> FunctionsAdapterResult<Self> {
        // Check if the series data type matches the expected type
        let expected_dtype = T::get_dtype();
        if series.data_type() != &expected_dtype {
            return Err(FunctionsAdapterError::TypeMismatch {
                expected: format!("{:?}", expected_dtype),
                actual: format!("{:?}", series.data_type()),
            });
        }

        let array = series
            .downcast::<daft_core::datatypes::DataArray<T>>()
            .map_err(|_| FunctionsAdapterError::IteratorError {
                message: format!(
                    "Failed to downcast Series to DataArray<{}> in PrimitiveSeriesIter",
                    std::any::type_name::<T>()
                ),
            })?;

        Ok(Self {
            array,
            index: 0,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T> Iterator for PrimitiveSeriesIter<'_, T>
where
    T: DaftPrimitiveType + DaftPhysicalType,
{
    type Item = Option<T::Native>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            let result = self.array.get(self.index);
            self.index += 1;
            Some(result)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.array.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

// Type aliases for convenience
pub type Int8SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Int8Type>;
pub type Int16SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Int16Type>;
pub type Int32SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Int32Type>;
pub type Int64SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Int64Type>;
pub type UInt8SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::UInt8Type>;
pub type UInt16SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::UInt16Type>;
pub type UInt32SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::UInt32Type>;
pub type UInt64SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::UInt64Type>;
pub type Float32SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Float32Type>;
pub type Float64SeriesIter<'a> = PrimitiveSeriesIter<'a, daft_core::datatypes::Float64Type>;

/// Extension trait to add iterator methods to Series
pub trait SeriesIterExt<'a> {
    /// Create an iterator over binary values
    fn iter_binary(&'a self) -> FunctionsAdapterResult<BinarySeriesIter<'a>>;

    /// Create an iterator over string values
    fn _iter_string(&'a self) -> FunctionsAdapterResult<StringSeriesIter<'a>>;

    /// Create an iterator over i8 values
    fn _iter_i8(&'a self) -> FunctionsAdapterResult<Int8SeriesIter<'a>>;

    /// Create an iterator over i16 values
    fn _iter_i16(&'a self) -> FunctionsAdapterResult<Int16SeriesIter<'a>>;

    /// Create an iterator over i32 values
    fn _iter_i32(&'a self) -> FunctionsAdapterResult<Int32SeriesIter<'a>>;

    /// Create an iterator over i64 values
    fn _iter_i64(&'a self) -> FunctionsAdapterResult<Int64SeriesIter<'a>>;

    /// Create an iterator over u8 values
    fn _iter_u8(&'a self) -> FunctionsAdapterResult<UInt8SeriesIter<'a>>;

    /// Create an iterator over u16 values
    fn _iter_u16(&'a self) -> FunctionsAdapterResult<UInt16SeriesIter<'a>>;

    /// Create an iterator over u32 values
    fn _iter_u32(&'a self) -> FunctionsAdapterResult<UInt32SeriesIter<'a>>;

    /// Create an iterator over u64 values
    fn _iter_u64(&'a self) -> FunctionsAdapterResult<UInt64SeriesIter<'a>>;

    /// Create an iterator over f32 values
    fn _iter_f32(&'a self) -> FunctionsAdapterResult<Float32SeriesIter<'a>>;

    /// Create an iterator over f64 values
    fn _iter_f64(&'a self) -> FunctionsAdapterResult<Float64SeriesIter<'a>>;
}

impl<'a> SeriesIterExt<'a> for Series {
    fn _iter_string(&'a self) -> FunctionsAdapterResult<StringSeriesIter<'a>> {
        StringSeriesIter::new(self)
    }

    fn iter_binary(&'a self) -> FunctionsAdapterResult<BinarySeriesIter<'a>> {
        BinarySeriesIter::new(self)
    }

    fn _iter_i8(&'a self) -> FunctionsAdapterResult<Int8SeriesIter<'a>> {
        Int8SeriesIter::new(self)
    }

    fn _iter_i16(&'a self) -> FunctionsAdapterResult<Int16SeriesIter<'a>> {
        Int16SeriesIter::new(self)
    }

    fn _iter_i32(&'a self) -> FunctionsAdapterResult<Int32SeriesIter<'a>> {
        Int32SeriesIter::new(self)
    }

    fn _iter_i64(&'a self) -> FunctionsAdapterResult<Int64SeriesIter<'a>> {
        Int64SeriesIter::new(self)
    }

    fn _iter_u8(&'a self) -> FunctionsAdapterResult<UInt8SeriesIter<'a>> {
        UInt8SeriesIter::new(self)
    }

    fn _iter_u16(&'a self) -> FunctionsAdapterResult<UInt16SeriesIter<'a>> {
        UInt16SeriesIter::new(self)
    }

    fn _iter_u32(&'a self) -> FunctionsAdapterResult<UInt32SeriesIter<'a>> {
        UInt32SeriesIter::new(self)
    }

    fn _iter_u64(&'a self) -> FunctionsAdapterResult<UInt64SeriesIter<'a>> {
        UInt64SeriesIter::new(self)
    }

    fn _iter_f32(&'a self) -> FunctionsAdapterResult<Float32SeriesIter<'a>> {
        Float32SeriesIter::new(self)
    }

    fn _iter_f64(&'a self) -> FunctionsAdapterResult<Float64SeriesIter<'a>> {
        Float64SeriesIter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use daft_core::{
        datatypes::{BinaryArray, Int32Array, Utf8Array},
        series::IntoSeries,
    };

    use super::*;

    #[test]
    fn test_string_iterator() {
        let string_array = Utf8Array::from_iter(
            "test",
            std::iter::once(Some("hello"))
                .chain(std::iter::once(None))
                .chain(std::iter::once(Some("world"))),
        );
        let series = string_array.into_series();
        let mut iter = series._iter_string().unwrap();

        assert_eq!(iter.next(), Some(Some("hello")));
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some("world")));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_binary_iterator() {
        let binary_array = BinaryArray::from_iter(
            "test",
            std::iter::once(Some(b"hello".as_slice()))
                .chain(std::iter::once(None))
                .chain(std::iter::once(Some(b"world".as_slice()))),
        );
        let series = binary_array.into_series();
        let mut iter = series.iter_binary().unwrap();

        assert_eq!(iter.next(), Some(Some(b"hello" as &[u8])));
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(b"world" as &[u8])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_int32_iterator() {
        let field = daft_core::datatypes::Field::new("test", daft_core::datatypes::DataType::Int32);
        let int_array =
            Int32Array::from_regular_iter(field, vec![Some(1), None, Some(3)].into_iter()).unwrap();
        let series = int_array.into_series();
        let mut iter = series._iter_i32().unwrap();

        assert_eq!(iter.next(), Some(Some(1)));
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(3)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_type_mismatch_error() {
        // Create a string series but try to iterate as i32
        let string_array = Utf8Array::from_iter(
            "test",
            std::iter::once(Some("hello")).chain(std::iter::once(Some("world"))),
        );
        let series = string_array.into_series();

        // This should return an error instead of panicking
        let result = series._iter_i32();
        assert!(result.is_err());

        // Check that it's the right error type
        match result {
            Err(FunctionsAdapterError::TypeMismatch { expected, actual }) => {
                assert!(expected.contains("Int32"));
                assert!(actual.contains("Utf8"));
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[test]
    fn test_string_type_mismatch_error() {
        // Create an int32 series but try to iterate as string
        let field = daft_core::datatypes::Field::new("test", daft_core::datatypes::DataType::Int32);
        let int_array =
            Int32Array::from_regular_iter(field, vec![Some(1), Some(2)].into_iter()).unwrap();
        let series = int_array.into_series();

        // This should return an error instead of panicking
        let result = series._iter_string();
        assert!(result.is_err());

        // Check that it's the right error type
        match result {
            Err(FunctionsAdapterError::TypeMismatch { expected, actual }) => {
                assert!(expected.contains("Utf8"));
                assert!(actual.contains("Int32"));
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[test]
    fn test_binary_type_mismatch_error() {
        // Create an int32 series but try to iterate as binary
        let field = daft_core::datatypes::Field::new("test", daft_core::datatypes::DataType::Int32);
        let int_array =
            Int32Array::from_regular_iter(field, vec![Some(1), Some(2)].into_iter()).unwrap();
        let series = int_array.into_series();

        // This should return an error instead of panicking
        let result = series.iter_binary();
        assert!(result.is_err());

        // Check that it's the right error type
        match result {
            Err(FunctionsAdapterError::TypeMismatch { expected, actual }) => {
                assert!(expected.contains("Binary"));
                assert!(actual.contains("Int32"));
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }
}
