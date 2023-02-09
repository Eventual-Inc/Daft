use arrow2::bitmap::utils::{BitmapIter, ZipValidity};

use crate::datatypes::{BooleanArray, DaftNumericType};

use super::DataArray;

impl<'a, T> IntoIterator for &'a DataArray<T>
where
    T: DaftNumericType,
{
    type Item = Option<&'a T::Native>;
    type IntoIter = ZipValidity<'a, &'a T::Native, std::slice::Iter<'a, T::Native>>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.downcast().into_iter()
    }
}

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<'a, bool, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.downcast().into_iter()
    }
}
