use daft_arrow::{
    array::{self, MonthsDaysNsArray},
    types::months_days_ns,
};

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray, IntervalArray,
        NullArray, NumericNative, Utf8Array,
        logical::{DateArray, DurationArray, TimeArray, TimestampArray},
    },
};

pub trait AsArrow {
    type Arrow2Output;
    type ArrowOutput;

    /// This does not correct for the logical types and will just yield the physical type of the array.
    /// For example, a TimestampArray will yield an arrow Int64Array rather than a arrow Timestamp Array.
    /// To get a corrected arrow type, see `.to_arrow()`.
    #[deprecated(note = "arrow2 migration")]
    fn as_arrow2(&self) -> &Self::Arrow2Output;
    fn as_arrow(&self) -> common_error::DaftResult<Self::ArrowOutput>;
}

impl<T> AsArrow for DataArray<T>
where
    T: DaftPrimitiveType,
{
    type Arrow2Output = array::PrimitiveArray<T::Native>;
    type ArrowOutput = arrow::array::PrimitiveArray<<T::Native as NumericNative>::ARROWTYPE>;

    // For DataArray<T: DaftNumericType>, retrieve the underlying Arrow2 PrimitiveArray.
    fn as_arrow2(&self) -> &Self::Arrow2Output {
        self.data().as_any().downcast_ref().unwrap()
    }

    fn as_arrow(&self) -> common_error::DaftResult<Self::ArrowOutput> {
        Ok(self
            .to_arrow()
            .as_any()
            .downcast_ref::<Self::ArrowOutput>()
            .ok_or_else(|| {
                common_error::DaftError::TypeError(
                    "Failed to downcast to arrow array type".to_string(),
                )
            })?
            .clone())
    }
}

macro_rules! impl_asarrow_dataarray {
    ($da:ident, $arrow2_output:ty, $arrow_output:ty) => {
        impl AsArrow for $da {
            type Arrow2Output = $arrow2_output;
            type ArrowOutput = $arrow_output;

            fn as_arrow2(&self) -> &Self::Arrow2Output {
                self.data().as_any().downcast_ref().unwrap()
            }

            fn as_arrow(&self) -> common_error::DaftResult<Self::ArrowOutput> {
                Ok(self
                    .to_arrow()
                    .as_any()
                    .downcast_ref::<Self::ArrowOutput>()
                    .ok_or_else(|| {
                        common_error::DaftError::TypeError(
                            "Failed to downcast to arrow array type".to_string(),
                        )
                    })?
                    .clone())
            }
        }
    };
}

macro_rules! impl_asarrow_logicalarray {
    ($da:ident, $arrow2_output:ty, $arrow_output:ty) => {
        impl AsArrow for $da {
            type Arrow2Output = $arrow2_output;
            type ArrowOutput = $arrow_output;

            fn as_arrow2(&self) -> &Self::Arrow2Output {
                self.physical.data().as_any().downcast_ref().unwrap()
            }

            fn as_arrow(&self) -> common_error::DaftResult<Self::ArrowOutput> {
                let arrow_arr = self.to_arrow()?;
                Ok(arrow_arr
                    .as_any()
                    .downcast_ref::<Self::ArrowOutput>()
                    .ok_or_else(|| {
                        common_error::DaftError::TypeError(
                            "Failed to downcast to arrow array type".to_string(),
                        )
                    })?
                    .clone())
            }
        }
    };
}
macro_rules! impl_asarrow_nested {
    ($da:ident, $arrow_output:ty) => {
        impl AsArrow for $da {
            type Arrow2Output = ();
            type ArrowOutput = $arrow_output;

            fn as_arrow2(&self) -> &Self::Arrow2Output {
                unimplemented!()
            }

            fn as_arrow(&self) -> common_error::DaftResult<Self::ArrowOutput> {
                let arrow_arr = self.to_arrow()?;
                Ok(arrow_arr
                    .as_any()
                    .downcast_ref::<Self::ArrowOutput>()
                    .ok_or_else(|| {
                        common_error::DaftError::TypeError(
                            "Failed to downcast to arrow array type".to_string(),
                        )
                    })?
                    .clone())
            }
        }
    };
}

impl_asarrow_dataarray!(NullArray, array::NullArray, arrow::array::NullArray);
impl_asarrow_dataarray!(
    Utf8Array,
    array::Utf8Array<i64>,
    arrow::array::LargeStringArray
);
impl_asarrow_dataarray!(
    BooleanArray,
    array::BooleanArray,
    arrow::array::BooleanArray
);
impl_asarrow_dataarray!(
    BinaryArray,
    array::BinaryArray<i64>,
    arrow::array::LargeBinaryArray
);
impl_asarrow_dataarray!(
    FixedSizeBinaryArray,
    array::FixedSizeBinaryArray,
    arrow::array::FixedSizeBinaryArray
);
impl_asarrow_dataarray!(
    IntervalArray,
    array::PrimitiveArray<months_days_ns>,
    MonthsDaysNsArray
);

impl_asarrow_logicalarray!(
    DateArray,
    array::PrimitiveArray<i32>,
    arrow::array::Date32Array
);
impl_asarrow_logicalarray!(
    TimeArray,
    array::PrimitiveArray<i64>,
    arrow::array::Time64NanosecondArray
);

impl_asarrow_logicalarray!(
    DurationArray,
    array::PrimitiveArray<i64>,
    arrow::array::DurationMillisecondArray
);

impl_asarrow_logicalarray!(
    TimestampArray,
    array::PrimitiveArray<i64>,
    arrow::array::TimestampMicrosecondArray
);

impl_asarrow_nested!(ListArray, arrow::array::LargeListArray);
impl_asarrow_nested!(FixedSizeListArray, arrow::array::FixedSizeListArray);
impl_asarrow_nested!(StructArray, arrow::array::StructArray);

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::Array;
    use common_error::DaftResult;
    use rstest::rstest;

    use crate::{prelude::*, series};

    macro_rules! test_primitive_into_arrow {
        ($test_name:ident, $array_type:ty, $values:expr) => {
            #[test]
            fn $test_name() -> DaftResult<()> {
                let values = $values.clone();
                let values_ref = values.as_slice();
                let arr = <$array_type>::from_values("test", $values.into_iter());
                let arrow_arr = arr.as_arrow()?;
                assert_eq!(arrow_arr.len(), 3);
                assert_eq!(arrow_arr.values(), values_ref);
                Ok(())
            }
        };
    }

    test_primitive_into_arrow!(test_into_arrow_int8, Int8Array, vec![1i8, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_int16, Int16Array, vec![1i16, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_int32, Int32Array, vec![1i32, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_int64, Int64Array, vec![1i64, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_uint8, UInt8Array, vec![1u8, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_uint16, UInt16Array, vec![1u16, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_uint32, UInt32Array, vec![1u32, 2, 3]);
    test_primitive_into_arrow!(test_into_arrow_uint64, UInt64Array, vec![1u64, 2, 3]);
    test_primitive_into_arrow!(
        test_into_arrow_float32,
        Float32Array,
        vec![1.0f32, 2.0, 3.0]
    );
    test_primitive_into_arrow!(
        test_into_arrow_float64,
        Float64Array,
        vec![1.0f64, 2.0, 3.0]
    );

    #[test]
    fn test_into_arrow_null() -> DaftResult<()> {
        let arr = NullArray::full_null("test", &DataType::Null, 3);
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Null);
        Ok(())
    }

    #[test]
    fn test_into_arrow_utf8() -> DaftResult<()> {
        let arr = Utf8Array::from_values("test", vec!["a", "b", "c"].into_iter());
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::LargeUtf8
        );
        Ok(())
    }

    #[test]
    fn test_into_arrow_boolean() -> DaftResult<()> {
        let arr = BooleanArray::from_values("test", vec![true, false, true].into_iter());
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Boolean);
        Ok(())
    }

    #[test]
    fn test_into_arrow_binary() -> DaftResult<()> {
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
    fn test_into_arrow_fixed_size_binary() -> DaftResult<()> {
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
    fn test_into_arrow_date() -> DaftResult<()> {
        let arr = DateArray::new(
            Field::new("test", DataType::Date),
            Int32Array::from_values("", vec![1, 2, 3].into_iter()),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(arrow_arr.data_type(), &arrow::datatypes::DataType::Date32);
        Ok(())
    }

    #[test]
    fn test_into_arrow_time() -> DaftResult<()> {
        let arr = TimeArray::new(
            Field::new("test", DataType::Time(TimeUnit::Nanoseconds)),
            Int64Array::from_values("", vec![1, 2, 3].into_iter()),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)
        );
        Ok(())
    }

    #[test]
    fn test_into_arrow_duration() -> DaftResult<()> {
        let arr = DurationArray::new(
            Field::new("test", DataType::Duration(TimeUnit::Milliseconds)),
            Int64Array::from_values("", vec![1000, 2000, 3000].into_iter()),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Millisecond)
        );
        Ok(())
    }

    #[test]
    fn test_into_arrow_timestamp() -> DaftResult<()> {
        let arr = TimestampArray::new(
            Field::new("test", DataType::Timestamp(TimeUnit::Microseconds, None)),
            Int64Array::from_values("", vec![1000000, 2000000, 3000000].into_iter()),
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        Ok(())
    }

    #[rstest]
    #[case(series![1u8, 2u8, 3u8])]
    #[case(series![1i8, 2i8, 3i8])]
    #[case(series![1i16, 2i16, 3i16])]
    #[case(series![1i32, 2i32, 3i32])]
    #[case(series![1i64, 2i64, 3i64])]
    #[case(series![1f32, 2f32, 3f32])]
    #[case(series![1f64, 2f64, 3f64])]
    #[case(series!["a", "b", "c"])]
    #[case(series![true, false, false])]
    #[case(Series::empty("test", &DataType::Null))]
    #[case(Series::empty("test", &DataType::Utf8))]
    #[case(Series::empty("test", &DataType::Int32))]
    #[case(Series::empty("test", &DataType::Float64))]
    fn test_into_arrow_list(#[case] data: Series) -> DaftResult<()> {
        let arr = ListArray::from_series("test", vec![Some(data.clone()), None])?;
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 2);
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::LargeList(Arc::new(data.field().to_arrow()?))
        );

        Ok(())
    }

    #[rstest]
    #[case(series![1u8, 2u8, 3u8])]
    #[case(series![1i8, 2i8, 3i8])]
    #[case(series![1i16, 2i16, 3i16])]
    #[case(series![1i32, 2i32, 3i32])]
    #[case(series![1i64, 2i64, 3i64])]
    #[case(series![1f32, 2f32, 3f32])]
    #[case(series![1f64, 2f64, 3f64])]
    #[case(series!["a", "b", "c"])]
    #[case(series![true, false, false])]
    #[case(Series::empty("literal", &DataType::Null))]
    #[case(Series::empty("literal", &DataType::Utf8))]
    #[case(Series::empty("literal", &DataType::Int32))]
    #[case(Series::empty("literal", &DataType::Float64))]
    fn test_into_arrow_fixed_size_list(#[case] data: Series) -> DaftResult<()> {
        let arr = FixedSizeListArray::new(
            Field::new(
                "test",
                DataType::FixedSizeList(Box::new(data.data_type().clone()), 1),
            ),
            data.clone(),
            None,
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), arr.len());
        assert_eq!(arrow_arr.null_count(), arr.null_count());
        assert_eq!(arrow_arr.value_length() as usize, arr.fixed_element_len());
        assert_eq!(
            arrow_arr.data_type(),
            &arrow::datatypes::DataType::FixedSizeList(Arc::new(data.field().to_arrow()?), 1)
        );

        Ok(())
    }

    #[test]
    fn test_into_arrow_struct() -> DaftResult<()> {
        let field1 = Int32Array::from_values("a", vec![1, 2, 3].into_iter());
        let field2 = Utf8Array::from_values("b", vec!["x", "y", "z"].into_iter());
        let arr = StructArray::new(
            Field::new(
                "test",
                DataType::Struct(vec![
                    Field::new("a", DataType::Int32),
                    Field::new("b", DataType::Utf8),
                ]),
            ),
            vec![field1.into_series(), field2.into_series()],
            None,
        );
        let arrow_arr = arr.as_arrow()?;
        assert_eq!(arrow_arr.len(), 3);
        Ok(())
    }
}
