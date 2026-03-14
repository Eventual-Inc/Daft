//! This was originally copied from arrow2.

use std::hint::unreachable_unchecked;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

/// A wrapper type of [`Vec<O>`] representing the invariants of Arrow's offsets.
/// It is guaranteed to (sound to assume that):
/// * every element is `>= 0`
/// * element at position `i` is >= than element at position `i-1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetsBuilder<O: OffsetSizeTrait>(Vec<O>);

impl<O: OffsetSizeTrait> Default for OffsetsBuilder<O> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OffsetSizeTrait> OffsetsBuilder<O> {
    /// Returns an empty [`OffsetsBuilder`] (i.e. with a single element, the zero)
    #[inline]
    pub(crate) fn new() -> Self {
        Self(vec![O::zero()])
    }

    /// Returns a new [`OffsetsBuilder`] with a capacity, allocating at least `capacity + 1`
    /// entries.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(O::zero());
        Self(offsets)
    }

    /// Reserves `additional` entries.
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Reserves exactly `additional` entries.
    pub(crate) fn reserve_exact(&mut self, additional: usize) {
        self.0.reserve_exact(additional);
    }

    /// Shrinks the capacity of self to fit.
    pub(crate) fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// Pushes a new element with a given length.
    /// # Error
    /// This function errors iff the new last item is larger than what `O` supports.
    /// # Implementation
    /// This function:
    /// * checks that this length does not overflow
    #[inline]
    pub(crate) fn try_push_usize(&mut self, length: usize) -> GeoArrowResult<()> {
        let length = O::usize_as(length);

        let old_length = self.last();
        // let new_length = old_length.checked_add(&length).ok_or(Error::Overflow)?;
        let new_length = *old_length + length;
        self.0.push(new_length);
        Ok(())
    }

    /// Returns the last offset of this container.
    #[inline]
    pub(crate) fn last(&self) -> &O {
        match self.0.last() {
            Some(element) => element,
            None => unsafe { unreachable_unchecked() },
        }
    }

    /// Returns the length an array with these offsets would be.
    #[inline]
    pub(crate) fn len_proxy(&self) -> usize {
        self.0.len() - 1
    }

    #[inline]
    /// Returns the number of offsets in this container.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the byte slice stored in this buffer
    #[inline]
    pub(crate) fn as_slice(&self) -> &[O] {
        self.0.as_slice()
    }

    /// Extends itself with `additional` elements equal to the last offset.
    /// This is useful to extend offsets with empty values, e.g. for null slots.
    #[inline]
    pub(crate) fn extend_constant(&mut self, additional: usize) {
        let offset = *self.last();
        if additional == 1 {
            self.0.push(offset)
        } else {
            self.0.resize(self.len() + additional, offset)
        }
    }

    pub(crate) fn finish(self) -> OffsetBuffer<O> {
        OffsetBuffer::new(self.0.into())
    }
}

impl From<OffsetsBuilder<i32>> for OffsetsBuilder<i64> {
    fn from(offsets: OffsetsBuilder<i32>) -> Self {
        // this conversion is lossless and uphelds all invariants
        Self(
            offsets
                .as_slice()
                .iter()
                .map(|x| *x as i64)
                .collect::<Vec<_>>(),
        )
    }
}

impl TryFrom<OffsetsBuilder<i64>> for OffsetsBuilder<i32> {
    type Error = GeoArrowError;

    fn try_from(offsets: OffsetsBuilder<i64>) -> GeoArrowResult<Self> {
        i32::try_from(*offsets.last()).map_err(|_| GeoArrowError::Overflow)?;

        // this conversion is lossless and uphelds all invariants
        Ok(Self(
            offsets
                .as_slice()
                .iter()
                .map(|x| *x as i32)
                .collect::<Vec<_>>(),
        ))
    }
}
