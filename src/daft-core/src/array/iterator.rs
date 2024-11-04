use arrow2::bitmap::utils::{BitmapIter, ZipValidity};

use super::{ops::as_arrow::AsArrow, DataArray};
use crate::datatypes::{BooleanArray, DaftPrimitiveType};

impl<'a, T> IntoIterator for &'a DataArray<T>
where
    T: DaftPrimitiveType,
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
