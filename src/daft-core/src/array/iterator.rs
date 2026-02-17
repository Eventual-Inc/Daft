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
