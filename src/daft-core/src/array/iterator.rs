use std::iter::{RepeatN, repeat_n};

use arrow::buffer::NullBuffer;

use crate::{
    array::{
        DataArray,
        prelude::{NullArray, Utf8Array},
    },
    datatypes::{BinaryArray, BooleanArray, DaftPrimitiveType, FixedSizeBinaryArray},
};

// ---- Primitive Iterator ----

/// Iterator over a `DataArray<T>` yielding `Option<T::Native>` (by value).
#[derive(Clone)]
pub struct PrimitiveIter<'a, N> {
    values: &'a [N],
    nulls: Option<&'a NullBuffer>,
    index: usize,
    len: usize,
}

impl<N: Copy> Iterator for PrimitiveIter<'_, N> {
    type Item = Option<N>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let i = self.index;
        self.index += 1;
        Some(if self.nulls.is_none_or(|n| n.is_valid(i)) {
            Some(self.values[i])
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.index;
        (rem, Some(rem))
    }
}

impl<N: Copy> DoubleEndedIterator for PrimitiveIter<'_, N> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        self.len -= 1;
        let i = self.len;
        Some(if self.nulls.is_none_or(|n| n.is_valid(i)) {
            Some(self.values[i])
        } else {
            None
        })
    }
}

impl<N: Copy> ExactSizeIterator for PrimitiveIter<'_, N> {}

impl<T: DaftPrimitiveType> DataArray<T> {
    pub fn iter(&self) -> PrimitiveIter<'_, T::Native> {
        PrimitiveIter {
            values: self.as_slice(),
            nulls: self.nulls(),
            index: 0,
            len: self.len(),
        }
    }
}

impl<'a, T: DaftPrimitiveType> IntoIterator for &'a DataArray<T> {
    type Item = Option<T::Native>;
    type IntoIter = PrimitiveIter<'a, T::Native>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// ---- Generic index-based iterator ----
// Used for Utf8, Binary, and FixedSizeBinary arrays.
// Stores a reference to the daft array and uses .get(idx) for value access,
// avoiding any direct dependency on arrow2 types.

#[derive(Clone)]
pub struct GenericArrayIter<'a, A, T> {
    array: &'a A,
    get_fn: fn(&'a A, usize) -> Option<T>,
    index: usize,
    len: usize,
}

impl<A, T> Iterator for GenericArrayIter<'_, A, T> {
    type Item = Option<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let i = self.index;
        self.index += 1;
        Some((self.get_fn)(self.array, i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.index;
        (rem, Some(rem))
    }
}

impl<A, T> DoubleEndedIterator for GenericArrayIter<'_, A, T> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        self.len -= 1;
        let i = self.len;
        Some((self.get_fn)(self.array, i))
    }
}

impl<A, T> ExactSizeIterator for GenericArrayIter<'_, A, T> {}

// ---- Boolean ----

/// Iterator over a `BooleanArray` yielding `Option<bool>`.
/// Reads bits directly from the underlying bitmap, avoiding per-element bounds checks.
#[derive(Clone)]
pub struct BooleanIter<'a> {
    // TODO(arrow2): Uses arrow2 Bitmap because there's no safe way to convert an
    // arrow2 &Bitmap into an arrow-rs &BooleanBuffer. Once BooleanArray is backed by arrow-rs, then this needs to be changed.
    values: &'a daft_arrow::bitmap::Bitmap,
    nulls: Option<&'a NullBuffer>,
    index: usize,
    len: usize,
}

impl Iterator for BooleanIter<'_> {
    type Item = Option<bool>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        let i = self.index;
        self.index += 1;
        Some(if self.nulls.is_none_or(|n| n.is_valid(i)) {
            Some(self.values.get_bit(i))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.index;
        (rem, Some(rem))
    }
}

impl DoubleEndedIterator for BooleanIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }
        self.len -= 1;
        let i = self.len;
        Some(if self.nulls.is_none_or(|n| n.is_valid(i)) {
            Some(self.values.get_bit(i))
        } else {
            None
        })
    }
}

impl ExactSizeIterator for BooleanIter<'_> {}

impl BooleanArray {
    pub fn iter(&self) -> BooleanIter<'_> {
        BooleanIter {
            values: self.as_bitmap(),
            nulls: self.nulls(),
            index: 0,
            len: self.len(),
        }
    }
}

impl<'a> IntoIterator for &'a BooleanArray {
    type Item = Option<bool>;
    type IntoIter = BooleanIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// ---- Utf8 ----

pub type Utf8Iter<'a> = GenericArrayIter<'a, Utf8Array, &'a str>;

impl Utf8Array {
    pub fn iter(&self) -> Utf8Iter<'_> {
        GenericArrayIter {
            array: self,
            get_fn: Self::get,
            index: 0,
            len: self.len(),
        }
    }
}

impl<'a> IntoIterator for &'a Utf8Array {
    type Item = Option<&'a str>;
    type IntoIter = Utf8Iter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// ---- Binary ----

pub type BinaryIter<'a> = GenericArrayIter<'a, BinaryArray, &'a [u8]>;

impl BinaryArray {
    pub fn iter(&self) -> BinaryIter<'_> {
        GenericArrayIter {
            array: self,
            get_fn: Self::get,
            index: 0,
            len: self.len(),
        }
    }
}

impl<'a> IntoIterator for &'a BinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = BinaryIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// ---- FixedSizeBinary ----

pub type FixedSizeBinaryIter<'a> = GenericArrayIter<'a, FixedSizeBinaryArray, &'a [u8]>;

impl FixedSizeBinaryArray {
    pub fn iter(&self) -> FixedSizeBinaryIter<'_> {
        GenericArrayIter {
            array: self,
            get_fn: Self::get,
            index: 0,
            len: self.len(),
        }
    }
}

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = FixedSizeBinaryIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

// ---- Null ----

impl NullArray {
    pub fn iter(&self) -> RepeatN<Option<()>> {
        repeat_n(None, self.len())
    }
}

impl IntoIterator for &'_ NullArray {
    type IntoIter = RepeatN<Option<()>>;
    type Item = <Self::IntoIter as IntoIterator>::Item;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use daft_schema::field::Field;

    use crate::{
        array::ops::full::FullNull,
        datatypes::{
            BinaryArray, BooleanArray, DataType, FixedSizeBinaryArray, Int64Array, NullType,
            Utf8Array,
        },
    };

    // ---- Primitive ----

    #[test]
    fn primitive_no_nulls() {
        let arr = Int64Array::from_slice("a", &[10, 20, 30]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some(10i64), Some(20), Some(30)]);
    }

    #[test]
    fn primitive_with_nulls() {
        let arr = Int64Array::from_iter(
            Field::new("a", DataType::Int64),
            [Some(1i64), None, Some(3)],
        );
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some(1i64), None, Some(3)]);
    }

    #[test]
    fn primitive_all_null() {
        let arr = Int64Array::from_iter(Field::new("a", DataType::Int64), [None::<i64>, None]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![None, None]);
    }

    #[test]
    fn primitive_empty() {
        let arr = Int64Array::from_iter(
            Field::new("a", DataType::Int64),
            std::iter::empty::<Option<i64>>(),
        );
        assert_eq!(arr.iter().len(), 0);
        assert_eq!(arr.iter().next(), None);
    }

    #[test]
    fn primitive_into_iter() {
        let arr = Int64Array::from_slice("a", &[1, 2]);
        let vals: Vec<_> = (&arr).into_iter().collect();
        assert_eq!(vals, vec![Some(1i64), Some(2)]);
    }

    #[test]
    fn primitive_double_ended() {
        let arr = Int64Array::from_slice("a", &[1, 2, 3]);
        let vals: Vec<_> = arr.iter().rev().collect();
        assert_eq!(vals, vec![Some(3i64), Some(2), Some(1)]);
    }

    #[test]
    fn primitive_double_ended_with_nulls() {
        let arr = Int64Array::from_iter(
            Field::new("a", DataType::Int64),
            [Some(1i64), None, Some(3)],
        );
        let vals: Vec<_> = arr.iter().rev().collect();
        assert_eq!(vals, vec![Some(3i64), None, Some(1)]);
    }

    #[test]
    fn primitive_exact_size() {
        let arr = Int64Array::from_slice("a", &[1, 2, 3]);
        let mut it = arr.iter();
        assert_eq!(it.len(), 3);
        it.next();
        assert_eq!(it.len(), 2);
        it.next_back();
        assert_eq!(it.len(), 1);
    }

    #[test]
    fn primitive_clone() {
        let arr = Int64Array::from_slice("a", &[1, 2, 3]);
        let mut it = arr.iter();
        it.next();
        let cloned = it.clone();
        assert_eq!(cloned.collect::<Vec<_>>(), vec![Some(2i64), Some(3)]);
    }

    // ---- Boolean ----

    #[test]
    fn boolean_no_nulls() {
        let arr = BooleanArray::from_slice("b", &[true, false, true]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some(true), Some(false), Some(true)]);
    }

    #[test]
    fn boolean_with_nulls() {
        let arr = BooleanArray::from_iter("b", [Some(true), None, Some(false)].into_iter());
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some(true), None, Some(false)]);
    }

    #[test]
    fn boolean_empty() {
        let arr = BooleanArray::from_iter("b", std::iter::empty::<Option<bool>>());
        assert_eq!(arr.iter().len(), 0);
    }

    #[test]
    fn boolean_double_ended() {
        let arr = BooleanArray::from_iter("b", [Some(true), None, Some(false)].into_iter());
        let vals: Vec<_> = arr.iter().rev().collect();
        assert_eq!(vals, vec![Some(false), None, Some(true)]);
    }

    // ---- Utf8 ----

    #[test]
    fn utf8_no_nulls() {
        let arr = Utf8Array::from_slice("s", &["hello", "world"]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some("hello"), Some("world")]);
    }

    #[test]
    fn utf8_with_nulls() {
        let arr = Utf8Array::from_iter("s", [Some("a"), None, Some("c")]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some("a"), None, Some("c")]);
    }

    #[test]
    fn utf8_empty() {
        let arr = Utf8Array::from_iter("s", std::iter::empty::<Option<&str>>());
        assert_eq!(arr.iter().len(), 0);
    }

    #[test]
    fn utf8_double_ended() {
        let arr = Utf8Array::from_slice("s", &["a", "b", "c"]);
        let vals: Vec<_> = arr.iter().rev().collect();
        assert_eq!(vals, vec![Some("c"), Some("b"), Some("a")]);
    }

    // ---- Binary ----

    #[test]
    fn binary_no_nulls() {
        let arr = BinaryArray::from_values("b", [b"ab".as_slice(), b"cd"]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![Some(b"ab".as_slice()), Some(b"cd".as_slice())]);
    }

    #[test]
    fn binary_with_nulls() {
        let arr =
            BinaryArray::from_iter("b", [Some(b"ab".as_slice()), None, Some(b"ef".as_slice())]);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(
            vals,
            vec![Some(b"ab".as_slice()), None, Some(b"ef".as_slice())]
        );
    }

    // ---- FixedSizeBinary ----

    #[test]
    fn fixed_size_binary_no_nulls() {
        let arr = FixedSizeBinaryArray::from_iter(
            "fb",
            [Some([1u8, 2u8]), Some([3u8, 4u8])].into_iter(),
            2,
        );
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(
            vals,
            vec![Some([1u8, 2u8].as_slice()), Some([3u8, 4u8].as_slice())]
        );
    }

    #[test]
    fn fixed_size_binary_with_nulls() {
        let arr = FixedSizeBinaryArray::from_iter(
            "fb",
            [Some([1u8, 2u8]), None, Some([5u8, 6u8])].into_iter(),
            2,
        );
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(
            vals,
            vec![
                Some([1u8, 2u8].as_slice()),
                None,
                Some([5u8, 6u8].as_slice())
            ]
        );
    }

    // ---- Null ----

    #[test]
    fn null_array() {
        use crate::array::DataArray;
        let arr = DataArray::<NullType>::full_null("n", &DataType::Null, 3);
        let vals: Vec<_> = arr.iter().collect();
        assert_eq!(vals, vec![None, None, None]);
    }

    #[test]
    fn null_array_empty() {
        use crate::array::DataArray;
        let arr = DataArray::<NullType>::full_null("n", &DataType::Null, 0);
        assert_eq!(arr.iter().len(), 0);
    }
}
