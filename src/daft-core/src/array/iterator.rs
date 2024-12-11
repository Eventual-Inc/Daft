use std::{
    iter::{repeat, Repeat, Take},
    slice::{ChunksExact, Iter},
};

use arrow2::{
    array::ArrayValuesIter,
    bitmap::utils::{BitmapIter, ZipValidity},
};

use super::{
    ops::as_arrow::AsArrow,
    prelude::{NullArray, Utf8Array},
    DataArray,
};
use crate::datatypes::{BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray};

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

impl<'a> IntoIterator for &'a BooleanArray {
    type IntoIter = ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}

impl<'a> IntoIterator for &'a BinaryArray {
    type IntoIter =
        ZipValidity<&'a [u8], ArrayValuesIter<'a, arrow2::array::BinaryArray<i64>>, BitmapIter<'a>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type IntoIter = ZipValidity<&'a [u8], ChunksExact<'a, u8>, BitmapIter<'a>>;
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

impl<'a> IntoIterator for &'a Utf8Array {
    type IntoIter =
        ZipValidity<&'a str, ArrayValuesIter<'a, arrow2::array::Utf8Array<i64>>, BitmapIter<'a>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}
