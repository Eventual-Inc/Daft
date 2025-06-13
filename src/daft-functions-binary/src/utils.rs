use std::iter;

use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::DataArray,
    prelude::{AsArrow, BinaryArray, DaftIntegerType, DaftNumericType, FixedSizeBinaryArray},
};

pub(crate) enum BroadcastedBinaryIter<'a> {
    Repeat(std::iter::RepeatN<Option<&'a [u8]>>),
    NonRepeat(
        ZipValidity<
            &'a [u8],
            arrow2::array::ArrayValuesIter<'a, arrow2::array::BinaryArray<i64>>,
            arrow2::bitmap::utils::BitmapIter<'a>,
        >,
    ),
}

pub(crate) enum BroadcastedFixedSizeBinaryIter<'a> {
    Repeat(std::iter::RepeatN<Option<&'a [u8]>>),
    NonRepeat(ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>>),
}

pub(crate) enum BroadcastedNumericIter<'a, T: 'a, U>
where
    T: DaftIntegerType,
    T::Native: TryInto<U> + Ord,
{
    Repeat(
        std::iter::RepeatN<Option<<T as DaftNumericType>::Native>>,
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

pub(crate) fn create_broadcasted_binary_iter(
    arr: &BinaryArray,
    len: usize,
) -> BroadcastedBinaryIter<'_> {
    if arr.len() == 1 {
        BroadcastedBinaryIter::Repeat(std::iter::repeat_n(arr.as_arrow().get(0), len))
    } else {
        BroadcastedBinaryIter::NonRepeat(arr.as_arrow().iter())
    }
}

pub(crate) fn create_broadcasted_fixed_size_binary_iter(
    arr: &FixedSizeBinaryArray,
    len: usize,
) -> BroadcastedFixedSizeBinaryIter<'_> {
    if arr.len() == 1 {
        BroadcastedFixedSizeBinaryIter::Repeat(iter::repeat_n(arr.as_arrow().get(0), len))
    } else {
        BroadcastedFixedSizeBinaryIter::NonRepeat(arr.as_arrow().iter())
    }
}

pub(crate) fn create_broadcasted_numeric_iter<T, O>(
    arr: &DataArray<T>,
    len: usize,
) -> BroadcastedNumericIter<T, O>
where
    T: DaftIntegerType,
    T::Native: TryInto<O> + Ord,
{
    if arr.len() == 1 {
        BroadcastedNumericIter::Repeat(
            iter::repeat_n(arr.as_arrow().get(0), len),
            std::marker::PhantomData,
        )
    } else {
        let x = arr.as_arrow().iter();
        BroadcastedNumericIter::NonRepeat(x, std::marker::PhantomData)
    }
}
