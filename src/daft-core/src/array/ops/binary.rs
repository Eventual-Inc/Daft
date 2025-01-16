use std::iter;

use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use common_error::{DaftError, DaftResult};

use crate::{
    array::ops::as_arrow::AsArrow,
    datatypes::{
        BinaryArray, DaftIntegerType, DaftNumericType, DataArray, FixedSizeBinaryArray, UInt64Array,
    },
};

enum BroadcastedBinaryIter<'a> {
    Repeat(std::iter::Take<std::iter::Repeat<Option<&'a [u8]>>>),
    NonRepeat(
        ZipValidity<
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

enum BroadcastedNumericIter<'a, T: 'a, U>
where
    T: DaftIntegerType,
    T::Native: TryInto<U> + Ord,
{
    Repeat(
        std::iter::Take<std::iter::Repeat<Option<<T as DaftNumericType>::Native>>>,
        std::marker::PhantomData<U>,
    ),
    NonRepeat(
        ZipValidity<
            &'a <T as DaftNumericType>::Native,
            std::slice::Iter<'a, <T as DaftNumericType>::Native>,
            BitmapIter<'a>,
        >,
        std::marker::PhantomData<U>,
    ),
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

impl<'a, T: 'a, U> Iterator for BroadcastedNumericIter<'a, T, U>
where
    T: DaftIntegerType + Clone,
    T::Native: TryInto<U> + Ord,
{
    type Item = DaftResult<Option<U>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastedNumericIter::Repeat(iter, _) => iter.next().map(|x| {
                x.map(|x| {
                    x.try_into().map_err(|_| {
                        DaftError::ComputeError(
                            "Failed to cast numeric value to target type".to_string(),
                        )
                    })
                })
                .transpose()
            }),
            BroadcastedNumericIter::NonRepeat(iter, _) => iter.next().map(|x| {
                x.map(|x| {
                    (*x).try_into().map_err(|_| {
                        DaftError::ComputeError(
                            "Failed to cast numeric value to target type".to_string(),
                        )
                    })
                })
                .transpose()
            }),
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

fn create_broadcasted_fixed_size_binary_iter(
    arr: &FixedSizeBinaryArray,
    len: usize,
) -> BroadcastedFixedSizeBinaryIter<'_> {
    if arr.len() == 1 {
        BroadcastedFixedSizeBinaryIter::Repeat(iter::repeat(arr.as_arrow().get(0)).take(len))
    } else {
        BroadcastedFixedSizeBinaryIter::NonRepeat(arr.as_arrow().iter())
    }
}

fn create_broadcasted_numeric_iter<T, O>(
    arr: &DataArray<T>,
    len: usize,
) -> BroadcastedNumericIter<T, O>
where
    T: DaftIntegerType,
    T::Native: TryInto<O> + Ord,
{
    if arr.len() == 1 {
        BroadcastedNumericIter::Repeat(
            iter::repeat(arr.as_arrow().get(0)).take(len),
            std::marker::PhantomData,
        )
    } else {
        let x = arr.as_arrow().iter();
        BroadcastedNumericIter::NonRepeat(x, std::marker::PhantomData)
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

        let mut builder = arrow2::array::MutableBinaryArray::<i64>::new();

        let arrow_result = match length {
            Some(length) => {
                let length_iter = create_broadcasted_numeric_iter::<J, usize>(length, output_len);

                for ((val, start), length) in self_iter.zip(start_iter).zip(length_iter) {
                    match (val, start?, length?) {
                        (Some(val), Some(start), Some(length)) => {
                            if start >= val.len() || length == 0 {
                                builder.push(Some(&[]));
                            } else {
                                let end = (start + length).min(val.len());
                                let slice = &val[start..end];
                                builder.push(Some(slice));
                            }
                        }
                        _ => {
                            builder.push::<&[u8]>(None);
                        }
                    }
                }
                builder.into()
            }
            None => {
                for (val, start) in self_iter.zip(start_iter) {
                    match (val, start?) {
                        (Some(val), Some(start)) => {
                            if start >= val.len() {
                                builder.push(Some(&[]));
                            } else {
                                let slice = &val[start..];
                                builder.push(Some(slice));
                            }
                        }
                        _ => {
                            builder.push::<&[u8]>(None);
                        }
                    }
                }
                builder.into()
            }
        };

        Ok(Self::from((self.name(), Box::new(arrow_result))))
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

    pub fn binary_concat(&self, other: &Self) -> std::result::Result<Self, DaftError> {
        let self_arrow = self.as_arrow();
        let other_arrow = other.as_arrow();
        let self_size = self_arrow.size();
        let other_size = other_arrow.size();
        let combined_size = self_size + other_size;

        // Create a new FixedSizeBinaryArray with the combined size
        let mut values = Vec::with_capacity(self_arrow.len() * combined_size);
        let mut validity = arrow2::bitmap::MutableBitmap::new();

        let output_len = if self_arrow.len() == 1 || other_arrow.len() == 1 {
            std::cmp::max(self_arrow.len(), other_arrow.len())
        } else {
            self_arrow.len()
        };

        let self_iter = create_broadcasted_fixed_size_binary_iter(self, output_len);
        let other_iter = create_broadcasted_fixed_size_binary_iter(other, output_len);

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
}
