use arrow2::bitmap::utils::{BitmapIter, ZipValidity};

use crate::datatypes::{BooleanArray, DaftNumericType};

use super::DataArray;

use super::ops::as_arrow::AsArrow;

impl<'a, T> IntoIterator for &'a DataArray<T>
where
    T: DaftNumericType,
{
    type Item = Option<&'a T::Native>;
    type IntoIter = ZipValidity<&'a T::Native, std::slice::Iter<'a, T::Native>, BitmapIter<'a>>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<bool, BitmapIter<'a>, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_arrow().into_iter()
    }
}
