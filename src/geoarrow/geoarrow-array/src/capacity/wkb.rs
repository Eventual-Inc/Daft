use std::ops::Add;

use arrow_array::OffsetSizeTrait;
use geo_traits::GeometryTrait;
use wkb::writer::geometry_wkb_size;

/// A counter for the buffer sizes of a [`GenericWkbArray`][crate::array::GenericWkbArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct WkbCapacity {
    pub(crate) buffer_capacity: usize,
    pub(crate) offsets_capacity: usize,
}

impl WkbCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(buffer_capacity: usize, offsets_capacity: usize) -> Self {
        Self {
            buffer_capacity,
            offsets_capacity,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer_capacity == 0 && self.offsets_capacity == 0
    }

    /// The capacity of the underlying data buffer
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    /// The capacity of the underlying offsets buffer
    pub fn offsets_capacity(&self) -> usize {
        self.offsets_capacity
    }

    /// Add a Geometry to this capacity counter.
    #[inline]
    pub fn add_geometry<'a>(&mut self, geom: Option<&'a (impl GeometryTrait<T = f64> + 'a)>) {
        if let Some(geom) = geom {
            self.buffer_capacity += geometry_wkb_size(geom);
        }
        self.offsets_capacity += 1;
    }

    /// Create a capacity counter from an iterator of Geometries.
    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom);
        }
        counter
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self) -> usize {
        let offsets_byte_width = if O::IS_LARGE { 8 } else { 4 };
        let num_offsets = self.offsets_capacity;
        (offsets_byte_width * num_offsets) + self.buffer_capacity
    }
}

impl Default for WkbCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for WkbCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let buffer_capacity = self.buffer_capacity + rhs.buffer_capacity;
        let offsets_capacity = self.offsets_capacity + rhs.offsets_capacity;

        Self::new(buffer_capacity, offsets_capacity)
    }
}
