/// A vector with a fixed capacity, optimized for performance.
pub struct FixedCapacityVec {
    data: Box<[u8]>,
    len: usize,
}

impl FixedCapacityVec {
    /// Creates a new `FixedCapacityVec` with the specified capacity.
    #[inline]
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0; capacity].into_boxed_slice(),
            len: 0,
        }
    }

    /// Returns the current length of the vector.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the vector is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the capacity of the vector.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Pushes an element onto the end of the vector.
    ///
    /// # Panics
    ///
    /// Panics if the vector is already at capacity.
    #[inline]
    pub fn push(&mut self, value: u8) {
        assert!(self.len < self.capacity(), "FixedCapacityVec is at capacity");
        self.data[self.len] = value;
        self.len += 1;
    }

    /// Removes and returns the last element of the vector.
    ///
    /// # Panics
    ///
    /// Panics if the vector is empty.
    #[inline]
    pub fn pop(&mut self) -> u8 {
        assert!(!self.is_empty(), "FixedCapacityVec is empty");
        self.len -= 1;
        self.data[self.len]
    }

    /// Returns a reference to the element at the given index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> &u8 {
        assert!(index < self.len, "Index out of bounds");
        &self.data[index]
    }

    /// Returns a mutable reference to the element at the given index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> &mut u8 {
        assert!(index < self.len, "Index out of bounds");
        &mut self.data[index]
    }

    /// Clears the vector, removing all elements.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Returns a slice containing the entire capacity of the vector.
    ///
    /// This includes both initialized and uninitialized elements.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it returns a slice that may contain
    /// uninitialized memory. The caller must ensure that they only access
    /// initialized elements (up to `self.len()`).
    #[inline]
    pub fn capacity_slice(&self) -> &[u8] {
        &self.data
    }

    /// Returns a mutable slice containing the entire capacity of the vector.
    ///
    /// This includes both initialized and uninitialized elements.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it returns a slice that may contain
    /// uninitialized memory. The caller must ensure that they only access
    /// initialized elements (up to `self.len()`).
    #[inline]
    pub fn capacity_slice_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
