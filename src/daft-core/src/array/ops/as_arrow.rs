use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray, IntervalArray,
        NullArray, NumericNative, Utf8Array,
        logical::{DateArray, DurationArray, TimeArray, TimestampArray},
    },
};

pub trait AsArrow {
    type ArrowOutput;
    /// This does not correct for the logical types and will just yield the physical type of the array.
    /// For example, a TimestampArray will yield an arrow Int64Array rather than a arrow Timestamp Array.
    /// To get a corrected arrow type, see `.to_arrow()`.
    fn as_arrow(&self) -> common_error::DaftResult<&Self::ArrowOutput>;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftPrimitiveType,
{
    type ArrowOutput = arrow::array::PrimitiveArray<<T::Native as NumericNative>::ARROWTYPE>;

    fn as_arrow(&self) -> common_error::DaftResult<&Self::ArrowOutput> {
        self.data
            .as_any()
            .downcast_ref::<Self::ArrowOutput>()
            .ok_or_else(|| {
                common_error::DaftError::TypeError(
                    "Failed to downcast to arrow array type".to_string(),
                )
            })
    }
}

macro_rules! impl_asarrow_dataarray {
    ($da:ident, $arrow_output:ty) => {
        impl AsArrow for $da {
            type ArrowOutput = $arrow_output;

            fn as_arrow(&self) -> common_error::DaftResult<&Self::ArrowOutput> {
                self.data
                    .as_any()
                    .downcast_ref::<Self::ArrowOutput>()
                    .ok_or_else(|| {
                        common_error::DaftError::TypeError(
                            "Failed to downcast to arrow array type".to_string(),
                        )
                    })
            }
        }
    };
}

macro_rules! impl_asarrow_logicalarray {
    ($da:ident, $arrow_output:ty) => {
        impl AsArrow for $da {
            type ArrowOutput = $arrow_output;

            fn as_arrow(&self) -> common_error::DaftResult<&Self::ArrowOutput> {
                self.physical
                    .data
                    .as_any()
                    .downcast_ref::<Self::ArrowOutput>()
                    .ok_or_else(|| {
                        common_error::DaftError::TypeError(
                            "Failed to downcast to arrow array type".to_string(),
                        )
                    })
            }
        }
    };
}
macro_rules! impl_asarrow_nested {
    ($da:ident, $arrow_output:ty) => {
        impl AsArrow for $da {
            type ArrowOutput = $arrow_output;

            fn as_arrow(&self) -> common_error::DaftResult<&Self::ArrowOutput> {
                unimplemented!()
            }
        }
    };
}

impl_asarrow_dataarray!(NullArray, arrow::array::NullArray);
impl_asarrow_dataarray!(Utf8Array, arrow::array::LargeStringArray);
impl_asarrow_dataarray!(BooleanArray, arrow::array::BooleanArray);
impl_asarrow_dataarray!(BinaryArray, arrow::array::LargeBinaryArray);
impl_asarrow_dataarray!(FixedSizeBinaryArray, arrow::array::FixedSizeBinaryArray);
impl_asarrow_dataarray!(IntervalArray, arrow::array::IntervalMonthDayNanoArray);

impl_asarrow_logicalarray!(DateArray, arrow::array::Int32Array);
impl_asarrow_logicalarray!(TimeArray, arrow::array::Int64Array);
impl_asarrow_logicalarray!(DurationArray, arrow::array::Int64Array);
impl_asarrow_logicalarray!(TimestampArray, arrow::array::Int64Array);
impl_asarrow_nested!(ListArray, arrow::array::LargeListArray);
impl_asarrow_nested!(FixedSizeListArray, arrow::array::FixedSizeListArray);
impl_asarrow_nested!(StructArray, arrow::array::StructArray);

#[cfg(test)]
mod test {
    use arrow::array::Array;
    use common_error::DaftResult;

    use crate::prelude::*;

    macro_rules! test_primitive_as_arrow {
        ($test_name:ident, $array_type:ty, $values:expr) => {
            #[test]
            fn $test_name() -> DaftResult<()> {
                let values = $values.clone();
                let values_ref = values.as_slice();
                let arr = <$array_type>::from_slice("test", values_ref);

                let arrow_arr = arr.as_arrow()?;
                assert_eq!(arrow_arr.len(), 3);
                assert_eq!(arrow_arr.values(), values_ref);
                Ok(())
            }
        };
    }

    test_primitive_as_arrow!(test_as_arrow_int8, Int8Array, vec![1i8, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_int16, Int16Array, vec![1i16, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_int32, Int32Array, vec![1i32, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_int64, Int64Array, vec![1i64, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_uint8, UInt8Array, vec![1u8, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_uint16, UInt16Array, vec![1u16, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_uint32, UInt32Array, vec![1u32, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_uint64, UInt64Array, vec![1u64, 2, 3]);
    test_primitive_as_arrow!(test_as_arrow_float32, Float32Array, vec![1.0f32, 2.0, 3.0]);
    test_primitive_as_arrow!(test_as_arrow_float64, Float64Array, vec![1.0f64, 2.0, 3.0]);

    #[test]
    fn test_as_arrow_null() -> DaftResult<()> {
        let arr = NullArray::full_null("test", &DataType::Null, 3);
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Null);
        Ok(())
    }

    #[test]
    fn test_as_arrow_utf8() -> DaftResult<()> {
        let arr = Utf8Array::from_slice("test", &["a", "b", "c"]);
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::LargeUtf8
        );
        Ok(())
    }

    #[test]
    fn test_as_arrow_boolean() -> DaftResult<()> {
        let arr = BooleanArray::from_vec("test", vec![true, false, true]);
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Boolean);
        Ok(())
    }

    #[test]
    fn test_as_arrow_binary() -> DaftResult<()> {
        let arr = BinaryArray::from_values("test", vec![b"a".as_slice(), b"b", b"c"].into_iter());
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::LargeBinary
        );
        Ok(())
    }

    #[test]
    fn test_as_arrow_fixed_size_binary() -> DaftResult<()> {
        let arr = FixedSizeBinaryArray::from_iter(
            "test",
            vec![
                Some(b"ab".to_vec()),
                Some(b"cd".to_vec()),
                Some(b"ef".to_vec()),
                Some(b"ef".to_vec()),
            ]
            .into_iter(),
            2,
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 4);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::FixedSizeBinary(2)
        );
        Ok(())
    }

    #[test]
    fn test_as_arrow_date() -> DaftResult<()> {
        let arr = DateArray::new(
            Field::new("test", DataType::Date),
            Int32Array::from_slice("", &[1, 2, 3]),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Int32);
        Ok(())
    }

    #[test]
    fn test_as_arrow_time() -> DaftResult<()> {
        let arr = TimeArray::new(
            Field::new("test", DataType::Time(TimeUnit::Nanoseconds)),
            Int64Array::from_slice("", &[1, 2, 3]),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Int64);
        Ok(())
    }

    #[test]
    fn test_as_arrow_duration() -> DaftResult<()> {
        let arr = DurationArray::new(
            Field::new("test", DataType::Duration(TimeUnit::Milliseconds)),
            Int64Array::from_slice("", &[1000, 2000, 3000]),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Int64);
        Ok(())
    }

    #[test]
    fn test_as_arrow_timestamp() -> DaftResult<()> {
        let arr = TimestampArray::new(
            Field::new("test", DataType::Timestamp(TimeUnit::Microseconds, None)),
            Int64Array::from_slice("", &[1000000, 2000000, 3000000]),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Int64);
        Ok(())
    }
}
