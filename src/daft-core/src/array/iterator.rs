use std::{
    iter::{repeat, Repeat, Take},
    slice::{ChunksExact, Iter},
};

use arrow2::{
    array::ArrayValuesIter,
    bitmap::utils::{BitmapIter, ZipValidity},
};

use crate::{
    array::{
        ops::as_arrow::AsArrow,
        prelude::{NullArray, Utf8Array},
        DataArray,
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
                self.as_arrow().into_iter()
            }
        }
    };
}

// yields `bool`s
impl_into_iter!(BooleanArray, ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>);

// both yield `&[u8]`s
impl_into_iter!(
    BinaryArray,
    ZipValidity<&'a [u8], ArrayValuesIter<'a, arrow2::array::BinaryArray<i64>>, BitmapIter<'a>>,
);
impl_into_iter!(
    FixedSizeBinaryArray,
    ZipValidity<&'a [u8], ChunksExact<'a, u8>, BitmapIter<'a>>,
);

// yields `&str`s
impl_into_iter!(
    Utf8Array,
    ZipValidity<&'a str, ArrayValuesIter<'a, arrow2::array::Utf8Array<i64>>, BitmapIter<'a>>,
);

impl<'a, T> IntoIterator for &'a DataArray<T>
where
    T: DaftPrimitiveType,
{
    type IntoIter = ZipValidity<&'a T::Native, Iter<'a, T::Native>, BitmapIter<'a>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}

impl<'a> IntoIterator for &'a NullArray {
    type IntoIter = Take<Repeat<Option<()>>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        repeat(None).take(self.len())
    }
}
