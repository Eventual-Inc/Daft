#![expect(
    clippy::fallible_impl_from,
    reason = "TODO(andrewgazelka/others): This should really be changed in the future"
)]

use std::{borrow::Cow, sync::Arc};

use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::NumericNative,
    prelude::*,
    series::{ArrayWrapper, SeriesLike},
};

impl<T: DaftNumericType> From<(&str, Box<daft_arrow::array::PrimitiveArray<T::Native>>)>
    for DataArray<T>
{
    fn from(item: (&str, Box<daft_arrow::array::PrimitiveArray<T::Native>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, T::get_dtype()).into(), array).unwrap()
    }
}

impl From<(&str, Box<daft_arrow::array::NullArray>)> for NullArray {
    fn from(item: (&str, Box<daft_arrow::array::NullArray>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Null).into(), array).unwrap()
    }
}

impl From<(&str, Box<daft_arrow::array::Utf8Array<i64>>)> for Utf8Array {
    fn from(item: (&str, Box<daft_arrow::array::Utf8Array<i64>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Utf8).into(), array).unwrap()
    }
}

impl From<(&str, Box<daft_arrow::array::BinaryArray<i64>>)> for BinaryArray {
    fn from(item: (&str, Box<daft_arrow::array::BinaryArray<i64>>)) -> Self {
        let (name, array) = item;
        Self::new(Field::new(name, DataType::Binary).into(), array).unwrap()
    }
}

impl From<(&str, Box<daft_arrow::array::FixedSizeBinaryArray>)> for FixedSizeBinaryArray {
    fn from(item: (&str, Box<daft_arrow::array::FixedSizeBinaryArray>)) -> Self {
        let (name, array) = item;
        Self::new(
            Field::new(name, DataType::FixedSizeBinary(array.size())).into(),
            array,
        )
        .unwrap()
    }
}

impl<'a, I> From<(&str, I, usize)> for FixedSizeBinaryArray
where
    Cow<'a, [u8]>: From<I>,
{
    fn from((name, array, length): (&str, I, usize)) -> Self {
        let array = Cow::from(array);
        let array = array.into_owned();
        Self::new(
            Field::new(name, DataType::FixedSizeBinary(length)).into(),
            Box::new(daft_arrow::array::FixedSizeBinaryArray::new(
                daft_arrow::datatypes::DataType::FixedSizeBinary(length),
                daft_arrow::buffer::Buffer::from(array),
                None,
            )),
        )
        .unwrap()
    }
}

impl<T> From<(&str, &[T::Native])> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(item: (&str, &[T::Native])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(daft_arrow::array::PrimitiveArray::<T::Native>::from_slice(
            slice,
        ));
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl<T> From<(&str, Vec<T::Native>)> for DataArray<T>
where
    T: DaftNumericType,
{
    fn from(item: (&str, Vec<T::Native>)) -> Self {
        let (name, v) = item;
        let arrow_array = Box::new(daft_arrow::array::PrimitiveArray::<T::Native>::from_vec(v));
        Self::new(Field::new(name, T::get_dtype()).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[bool])> for BooleanArray {
    fn from(item: (&str, &[bool])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(daft_arrow::array::BooleanArray::from_slice(slice));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[Option<bool>])> for BooleanArray {
    fn from(item: (&str, &[Option<bool>])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(daft_arrow::array::BooleanArray::from_trusted_len_iter(
            slice.iter().copied(),
        ));
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, daft_arrow::array::BooleanArray)> for BooleanArray {
    fn from(item: (&str, daft_arrow::array::BooleanArray)) -> Self {
        let (name, arrow_array) = item;
        Self::new(
            Field::new(name, DataType::Boolean).into(),
            Box::new(arrow_array),
        )
        .unwrap()
    }
}

impl From<(&str, daft_arrow::bitmap::Bitmap)> for BooleanArray {
    fn from(item: (&str, daft_arrow::bitmap::Bitmap)) -> Self {
        let (name, bitmap) = item;
        Self::new(
            Field::new(name, DataType::Boolean).into(),
            Box::new(daft_arrow::array::BooleanArray::new(
                daft_arrow::datatypes::DataType::Boolean,
                bitmap,
                None,
            )),
        )
        .unwrap()
    }
}

impl From<(&str, Box<daft_arrow::array::BooleanArray>)> for BooleanArray {
    fn from(item: (&str, Box<daft_arrow::array::BooleanArray>)) -> Self {
        let (name, arrow_array) = item;
        Self::new(Field::new(name, DataType::Boolean).into(), arrow_array).unwrap()
    }
}

impl From<(&str, Vec<daft_arrow::types::months_days_ns>)> for IntervalArray {
    fn from(item: (&str, Vec<daft_arrow::types::months_days_ns>)) -> Self {
        let (name, vec) = item;
        let arrow_array = Box::new(daft_arrow::array::PrimitiveArray::<
            daft_arrow::types::months_days_ns,
        >::from_vec(vec));
        Self::new(Field::new(name, DataType::Interval).into(), arrow_array).unwrap()
    }
}

impl<T: AsRef<str>> From<(&str, &[T])> for DataArray<Utf8Type> {
    fn from(item: (&str, &[T])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(daft_arrow::array::Utf8Array::<i64>::from_slice(slice));
        Self::new(Field::new(name, DataType::Utf8).into(), arrow_array).unwrap()
    }
}

impl From<(&str, &[u8])> for BinaryArray {
    fn from(item: (&str, &[u8])) -> Self {
        let (name, slice) = item;
        let arrow_array = Box::new(daft_arrow::array::BinaryArray::<i64>::from_slice([slice]));
        Self::new(Field::new(name, DataType::Binary).into(), arrow_array).unwrap()
    }
}

impl<T: DaftPhysicalType, F: Into<Arc<Field>>> TryFrom<(F, Box<dyn daft_arrow::array::Array>)>
    for DataArray<T>
{
    type Error = DaftError;

    fn try_from(item: (F, Box<dyn daft_arrow::array::Array>)) -> DaftResult<Self> {
        let (field, array) = item;
        let field: Arc<Field> = field.into();
        Self::new(field, array)
    }
}

impl TryFrom<(&str, Vec<u8>, Vec<i64>)> for BinaryArray {
    type Error = DaftError;

    fn try_from(item: (&str, Vec<u8>, Vec<i64>)) -> DaftResult<Self> {
        let (name, data, offsets) = item;

        if offsets.is_empty() {
            return Err(DaftError::ValueError(
                "Expected non zero len offsets".to_string(),
            ));
        }
        let last_offset = *offsets.last().unwrap();
        if last_offset != data.len() as i64 {
            return Err(DaftError::ValueError(format!(
                "Expected Last offset in offsets to be the same as the length of the data array: {last_offset} vs {}",
                data.len()
            )));
        }

        assert_eq!(last_offset, data.len() as i64);
        let arrow_offsets = daft_arrow::offset::OffsetsBuffer::try_from(offsets)?;
        let bin_array = daft_arrow::array::BinaryArray::<i64>::try_new(
            daft_arrow::datatypes::DataType::LargeBinary,
            arrow_offsets,
            data.into(),
            None,
        )?;
        Self::new(
            Field::new(name, DataType::Binary).into(),
            Box::new(bin_array),
        )
    }
}

impl TryFrom<(&str, &[&Series])> for ListArray {
    type Error = DaftError;

    fn try_from(item: (&str, &[&Series])) -> DaftResult<Self> {
        let (name, data) = item;

        let lengths = data.iter().map(|s| s.len());
        let offsets = daft_arrow::offset::Offsets::try_from_lengths(lengths)?.into();
        let flat_child = Series::concat(data)?;

        Ok(Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            None,
        ))
    }
}

impl TryFrom<(&str, &[Option<&Series>])> for ListArray {
    type Error = DaftError;

    fn try_from(item: (&str, &[Option<&Series>])) -> DaftResult<Self> {
        let (name, data) = item;

        let lengths = data.iter().map(|s| s.map_or(0, Series::len));
        let offsets = daft_arrow::offset::Offsets::try_from_lengths(lengths)?.into();

        let validity = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        let flat_child = Series::concat(&data.iter().flatten().copied().collect::<Vec<_>>())?;

        Ok(Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(validity),
        ))
    }
}

impl TryFrom<(&str, &[Option<Series>])> for ListArray {
    type Error = DaftError;

    fn try_from(item: (&str, &[Option<Series>])) -> DaftResult<Self> {
        let (name, data) = item;

        let lengths = data.iter().map(|s| s.as_ref().map_or(0, |s| s.len()));
        let offsets = daft_arrow::offset::Offsets::try_from_lengths(lengths)?.into();

        let validity = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        let flat_child = Series::concat(&data.iter().flatten().collect::<Vec<_>>())?;

        Ok(Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(validity),
        ))
    }
}

impl ListArray {
    pub fn from_slice<T>(name: &str, data: &[Option<&[T]>]) -> Self
    where
        T: NumericNative,
        T::DAFTTYPE: DaftNumericType<Native = T>,
        ArrayWrapper<DataArray<T::DAFTTYPE>>: SeriesLike,
    {
        let flat_child_vec = data
            .iter()
            .copied()
            .flatten()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let flat_child = DataArray::<T::DAFTTYPE>::from((name, flat_child_vec)).into_series();

        let lengths = data.iter().map(|d| d.map_or(0, |d| d.len()));
        let offsets = daft_arrow::offset::Offsets::try_from_lengths(lengths)
            .unwrap()
            .into();

        let validity = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(validity),
        )
    }

    pub fn from_vec<T>(name: &str, data: Vec<Option<Vec<T>>>) -> Self
    where
        T: NumericNative,
        T::DAFTTYPE: DaftNumericType<Native = T>,
        ArrayWrapper<DataArray<T::DAFTTYPE>>: SeriesLike,
    {
        let flat_child_vec = data.iter().flatten().flatten().copied().collect::<Vec<_>>();
        let flat_child = DataArray::<T::DAFTTYPE>::from((name, flat_child_vec)).into_series();

        let lengths = data.iter().map(|d| d.as_ref().map_or(0, |d| d.len()));
        let offsets = daft_arrow::offset::Offsets::try_from_lengths(lengths)
            .unwrap()
            .into();

        let validity = daft_arrow::buffer::NullBuffer::from_iter(data.iter().map(Option::is_some));

        Self::new(
            flat_child.field().to_list_field().rename(name),
            flat_child,
            offsets,
            Some(validity),
        )
    }
}
