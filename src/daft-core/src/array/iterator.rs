use std::{
    iter::{RepeatN, repeat_n},
    slice::{ChunksExact, Iter},
};

use daft_arrow::{
    array::ArrayValuesIter,
    bitmap::utils::{BitmapIter, ZipValidity},
};

use crate::{
    array::{
        DataArray,
        ops::as_arrow::AsArrow,
        prelude::{NullArray, Utf8Array},
    },
    datatypes::{BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray},
};

macro_rules! impl_into_iter {
    (
        $array:ty
        , $into_iter:ty
        $(,)?
    ) => {
        impl<'a> IntoIterator for &'a $array {
            type IntoIter = $into_iter;
            type Item = <Self::IntoIter as IntoIterator>::Item;

            #[inline]
            fn into_iter(self) -> Self::IntoIter {
                self.as_arrow2().into_iter()
            }
        }
    };
}

// yields `bool`s
impl_into_iter!(BooleanArray, ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>);

// both yield `&[u8]`s
impl_into_iter!(
    BinaryArray,
    ZipValidity<&'a [u8], ArrayValuesIter<'a, daft_arrow::array::BinaryArray<i64>>, BitmapIter<'a>>,
);
impl_into_iter!(
    FixedSizeBinaryArray,
    ZipValidity<&'a [u8], ChunksExact<'a, u8>, BitmapIter<'a>>,
);

// yields `&str`s
impl_into_iter!(
    Utf8Array,
    ZipValidity<&'a str, ArrayValuesIter<'a, daft_arrow::array::Utf8Array<i64>>, BitmapIter<'a>>,
);

impl<'a, T> IntoIterator for &'a DataArray<T>
where
    T: DaftPrimitiveType,
{
    type IntoIter = ZipValidity<&'a T::Native, Iter<'a, T::Native>, BitmapIter<'a>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow2().into_iter()
    }
}

impl IntoIterator for &'_ NullArray {
    type IntoIter = RepeatN<Option<()>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        repeat_n(None, self.len())
    }
}
