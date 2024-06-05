//! Contains [`Buffer`], an immutable container for all Arrow physical types (e.g. i32, f64).

mod immutable;
mod iterator;

use crate::ffi::InternalArrowArray;
use std::ops::Deref;

pub(crate) enum BytesAllocator {
    #[allow(dead_code)]
    InternalArrowArray(InternalArrowArray),
}
pub(crate) type BytesInner<T> = foreign_vec::ForeignVec<BytesAllocator, T>;

/// Bytes representation.
#[repr(transparent)]
pub struct Bytes<T>(BytesInner<T>);

impl<T> Bytes<T> {
    /// Takes ownership of an allocated memory region.
    /// # Panics
    /// This function panics if and only if pointer is not null
    /// # Safety
    /// This function is safe if and only if `ptr` is valid for `length`
    /// # Implementation
    /// This function leaks if and only if `owner` does not deallocate
    /// the region `[ptr, ptr+length[` when dropped.
    #[inline]
    pub(crate) unsafe fn from_foreign(ptr: *const T, length: usize, owner: BytesAllocator) -> Self {
        Self(BytesInner::from_foreign(ptr, length, owner))
    }

    /// Returns a `Some` mutable reference of [`Vec<T>`] iff this was initialized
    /// from a [`Vec<T>`] and `None` otherwise.
    #[inline]
    pub(crate) fn get_vec(&mut self) -> Option<&mut Vec<T>> {
        self.0.get_vec()
    }
}

impl<T> Deref for Bytes<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<Vec<T>> for Bytes<T> {
    #[inline]
    fn from(data: Vec<T>) -> Self {
        let inner: BytesInner<T> = data.into();
        Bytes(inner)
    }
}

impl<T> From<BytesInner<T>> for Bytes<T> {
    #[inline]
    fn from(value: BytesInner<T>) -> Self {
        Self(value)
    }
}

pub(super) use iterator::IntoIter;

pub use immutable::Buffer;
