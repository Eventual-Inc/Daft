use std::iter;

use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use common_error::{DaftError, DaftResult};
use num_traits::Zero;

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{
        BinaryArray, DaftIntegerType, DaftNumericType, DataArray, FixedSizeBinaryArray, UInt64Array,
    },
};

enum BroadcastedBinaryIter<'a> {
    Repeat(std::iter::Take<std::iter::Repeat<Option<&'a [u8]>>>),
    NonRepeat(
        arrow2::bitmap::utils::ZipValidity<
            &'a [u8],
            arrow2::array::ArrayValuesIter<'a, arrow2::array::BinaryArray<i64>>,
            arrow2::bitmap::utils::BitmapIter<'a>,
        >,
    ),
}

enum BroadcastedFixedSizeBinaryIter<'a> {
    Repeat(std::iter::Take<std::iter::Repeat<Option<&'a [u8]>>>),
    NonRepeat(ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>>),
}

impl<'a> Iterator for BroadcastedFixedSizeBinaryIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedFixedSizeBinaryIter::Repeat(iter) => iter.next(),
            BroadcastedFixedSizeBinaryIter::NonRepeat(iter) => iter.next(),
        }
    }
}

impl<'a> Iterator for BroadcastedBinaryIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedBinaryIter::Repeat(iter) => iter.next(),
            BroadcastedBinaryIter::NonRepeat(iter) => iter.next(),
        }
    }
}

fn create_broadcasted_binary_iter(arr: &BinaryArray, len: usize) -> BroadcastedBinaryIter<'_> {
    if arr.len() == 1 {
        BroadcastedBinaryIter::Repeat(std::iter::repeat(arr.as_arrow().get(0)).take(len))
    } else {
        BroadcastedBinaryIter::NonRepeat(arr.as_arrow().iter())
    }
}

fn create_broadcasted_numeric_iter<'a, T, I>(
    arr: &'a DataArray<T>,
    len: usize,
) -> Box<dyn Iterator<Item = DaftResult<Option<I>>> + 'a>
where
    T: DaftIntegerType,
    T::Native: TryInto<I> + Ord + Zero,
{
    if arr.len() == 1 {
        let val = arr.as_arrow().iter().next().unwrap();
        Box::new(
            iter::repeat(val)
                .take(len)
                .map(|x| -> DaftResult<Option<I>> {
                    x.map(|x| {
                        (*x).try_into().map_err(|_| {
                            DaftError::ComputeError(
                                "Failed to cast numeric value to target type".to_string(),
                            )
                        })
                    })
                    .transpose()
                }),
        )
    } else {
        Box::new(arr.as_arrow().iter().map(|x| -> DaftResult<Option<I>> {
            x.map(|x| {
                (*x).try_into().map_err(|_| {
                    DaftError::ComputeError(
                        "Failed to cast numeric value to target type".to_string(),
                    )
                })
            })
            .transpose()
        }))
    }
}

impl BinaryArray {
    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let offsets = self_arrow.offsets();
        let arrow_result = arrow2::array::UInt64Array::from_iter(
            offsets.windows(2).map(|w| Some((w[1] - w[0]) as u64)),
        )
        .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    pub fn binary_concat(&self, other: &Self) -> DaftResult<Self> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();

        if self_arrow.len() == 0 || other_arrow.len() == 0 {
            return Ok(Self::from((
                self.name(),
                Box::new(arrow2::array::BinaryArray::<i64>::new_empty(
                    self_arrow.data_type().clone(),
                )),
            )));
        }

        let output_len = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            std::cmp::max(self_arrow.len(), other_arrow.len())
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_binary_iter(self, output_len);
        let other_iter = create_broadcasted_binary_iter(other, output_len);

        let arrow_result = self_iter
            .zip(other_iter)
            .map(|(left_val, right_val)| match (left_val, right_val) {
                (Some(left), Some(right)) => Some([left, right].concat()),
                _ => None,
            })
            .collect::<arrow2::array::BinaryArray<i64>>();

        Ok(Self::from((self.name(), Box::new(arrow_result))))
    }

    pub fn binary_slice<I, J>(
        &self,
        start: &DataArray<I>,
        length: Option<&DataArray<J>>,
    ) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord + TryInto<usize>,
        J: DaftIntegerType,
        <J as DaftNumericType>::Native: Ord + TryInto<usize>,
    {
        let self_arrow = self.as_arrow();
        let output_len = if self_arrow.len() == 1 {
            std::cmp::max(start.len(), length.map_or(1, |l| l.len()))
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_binary_iter(self, output_len);
        let start_iter = create_broadcasted_numeric_iter::<I, usize>(start, output_len);
        let length_iter = match length {
            Some(length) => create_broadcasted_numeric_iter::<J, usize>(length, output_len),
            None => Box::new(iter::repeat_with(|| Ok(None))),
        };

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        for ((val, start), length) in self_iter.zip(start_iter).zip(length_iter) {
            match (val, start?, length?) {
                (Some(val), Some(start), Some(length)) => {
                    if start >= val.len() || length == 0 {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let end = (start + length).min(val.len());
                        let slice = &val[start..end];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                (Some(val), Some(start), None) => {
                    if start >= val.len() {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let slice = &val[start..];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                _ => {
                    builder.push::<&[u8]>(None);
                    validity.push(false);
                }
            }
        }

        Ok(Self::from((self.name(), Box::new(builder.into()))))
    }
}

impl FixedSizeBinaryArray {
    pub fn length(&self) -> DaftResult<UInt64Array> {
        let self_arrow = self.as_arrow();
        let size = self_arrow.size();
        let arrow_result = arrow2::array::UInt64Array::from_iter(
            iter::repeat(Some(size as u64)).take(self_arrow.len()),
        )
        .with_validity(self_arrow.validity().cloned());
        Ok(UInt64Array::from((self.name(), Box::new(arrow_result))))
    }

    fn create_broadcasted_iter(&self, len: usize) -> BroadcastedFixedSizeBinaryIter<'_> {
        let self_arrow = self.as_arrow();
        if self_arrow.len() == 1 {
            BroadcastedFixedSizeBinaryIter::Repeat(iter::repeat(self_arrow.get(0)).take(len))
        } else {
            BroadcastedFixedSizeBinaryIter::NonRepeat(self_arrow.iter())
        }
    }

    pub fn binary_slice<I, J>(
        &self,
        start: &DataArray<I>,
        length: Option<&DataArray<J>>,
    ) -> DaftResult<BinaryArray>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: Ord + TryInto<usize>,
        J: DaftIntegerType,
        <J as DaftNumericType>::Native: Ord + TryInto<usize>,
    {
        let self_arrow = self.as_arrow();
        let output_len = if self_arrow.len() == 1 {
            std::cmp::max(start.len(), length.map_or(1, |l| l.len()))
        } else {
            self_arrow.len()
        };

        let self_iter = self.create_broadcasted_iter(output_len);
        let start_iter = create_broadcasted_numeric_iter::<I, usize>(start, output_len);
        let length_iter = match length {
            Some(length) => create_broadcasted_numeric_iter::<J, usize>(length, output_len),
            None => Box::new(iter::repeat_with(|| Ok(None))),
        };

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        for ((val, start), length) in self_iter.zip(start_iter).zip(length_iter) {
            match (val, start?, length?) {
                (Some(val), Some(start), Some(length)) => {
                    if start >= val.len() || length == 0 {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let end = (start + length).min(val.len());
                        let slice = &val[start..end];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                (Some(val), Some(start), None) => {
                    if start >= val.len() {
                        builder.push::<&[u8]>(None);
                        validity.push(false);
                    } else {
                        let slice = &val[start..];
                        builder.push(Some(slice));
                        validity.push(true);
                    }
                }
                _ => {
                    builder.push::<&[u8]>(None);
                    validity.push(false);
                }
            }
        }

        Ok(BinaryArray::from((self.name(), Box::new(builder.into()))))
    }

    pub fn binary_concat(&self, other: &Self) -> std::result::Result<Self, DaftError> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();
        let self_size = self_arrow.size();
        let other_size = other_arrow.size();
        let combined_size = self_size + other_size;

        // Create a new FixedSizeBinaryArray with the combined size
        let mut values = Vec::with_capacity(self_arrow.len() * combined_size);
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        let self_iter = self_arrow.iter();
        let other_iter = other_arrow.iter();

        for (val1, val2) in self_iter.zip(other_iter) {
            match (val1, val2) {
                (Some(val1), Some(val2)) => {
                    values.extend_from_slice(val1);
                    values.extend_from_slice(val2);
                    validity.push(true);
                }
                _ => {
                    values.extend(std::iter::repeat(0u8).take(combined_size));
                    validity.push(false);
                }
            }
        }

        // Create a new FixedSizeBinaryArray with the combined size
        let result = arrow2::array::FixedSizeBinaryArray::try_new(
            arrow2::datatypes::DataType::FixedSizeBinary(combined_size),
            values.into(),
            Some(validity.into()),
        )?;

        Ok(Self::from((self.name(), Box::new(result))))
    }

    pub fn into_binary(&self) -> std::result::Result<BinaryArray, DaftError> {
        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        for val in self.as_arrow() {
            match val {
                Some(val) => {
                    builder.push(Some(val));
                    validity.push(true);
                }
                None => {
                    builder.push::<&[u8]>(None);
                    validity.push(false);
                }
            }
        }

        Ok(BinaryArray::from((self.name(), Box::new(builder.into()))))
    }
}
