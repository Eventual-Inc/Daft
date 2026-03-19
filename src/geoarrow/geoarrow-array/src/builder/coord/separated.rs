use geo_traits::{CoordTrait, PointTrait};
use geoarrow_schema::{
    Dimension,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::array::SeparatedCoordBuffer;

/// The GeoArrow equivalent to `Vec<Option<Coord>>`: a mutable collection of coordinates.
///
/// This stores all coordinates in separated fashion as multiple arrays: `xxx` and `yyy`.
///
/// Converting an [`SeparatedCoordBufferBuilder`] into a [`SeparatedCoordBuffer`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct SeparatedCoordBufferBuilder {
    buffers: [Vec<f64>; 4],
    dim: Dimension,
}

impl SeparatedCoordBufferBuilder {
    /// Create a new empty builder with the given dimension
    pub fn new(dim: Dimension) -> Self {
        Self::with_capacity(0, dim)
    }

    /// Create a new builder with the given capacity and dimension
    pub fn with_capacity(capacity: usize, dim: Dimension) -> Self {
        // Only allocate buffers for existent dimensions
        let buffers = core::array::from_fn(|i| {
            if i < dim.size() {
                Vec::with_capacity(capacity)
            } else {
                Vec::new()
            }
        });

        Self { buffers, dim }
    }

    /// Initialize a buffer of a given length with all coordinates set to the given value
    pub fn initialize(len: usize, dim: Dimension, value: f64) -> Self {
        // Only allocate buffers for existent dimensions
        let buffers = core::array::from_fn(|i| {
            if i < dim.size() {
                vec![value; len]
            } else {
                Vec::new()
            }
        });

        Self { buffers, dim }
    }

    /// Reserves capacity for at least `additional` more coordinates.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.buffers
            .iter_mut()
            .for_each(|buffer| buffer.reserve(additional))
    }

    /// Reserves the minimum capacity for at least `additional` more coordinates.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, additional: usize) {
        self.buffers
            .iter_mut()
            .for_each(|buffer| buffer.reserve_exact(additional))
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.buffers
            .iter_mut()
            .for_each(|buffer| buffer.shrink_to_fit());
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.buffers[0].capacity()
    }

    /// The number of coordinates in this builder
    pub fn len(&self) -> usize {
        self.buffers[0].len()
    }

    /// Whether this builder is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn push_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.try_push_coord(coord).unwrap()
    }

    /// Push a new coord onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added coordinate does not have the same dimension as the coordinate buffer.
    pub fn try_push_coord(&mut self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<()> {
        // Note duplicated across buffer types; consider refactoring
        match self.dim {
            Dimension::XY => match coord.dim() {
                geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {}
                d => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "coord dimension must be XY for this buffer; got {d:?}."
                    )));
                }
            },
            Dimension::XYZ => match coord.dim() {
                geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => {}
                d => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "coord dimension must be XYZ for this buffer; got {d:?}."
                    )));
                }
            },
            Dimension::XYM => match coord.dim() {
                geo_traits::Dimensions::Xym | geo_traits::Dimensions::Unknown(3) => {}
                d => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "coord dimension must be XYM for this buffer; got {d:?}."
                    )));
                }
            },
            Dimension::XYZM => match coord.dim() {
                geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => {}
                d => {
                    return Err(GeoArrowError::IncorrectGeometryType(format!(
                        "coord dimension must be XYZM for this buffer; got {d:?}."
                    )));
                }
            },
        }

        self.buffers[0].push(coord.x());
        self.buffers[1].push(coord.y());
        if let Some(z) = coord.nth(2) {
            self.buffers[2].push(z);
        };
        if let Some(m) = coord.nth(3) {
            self.buffers[3].push(m);
        };
        Ok(())
    }

    /// Push a valid coordinate with the given constant value
    ///
    /// Used in the case of point and rect arrays, where a `null` array value still needs to have
    /// space allocated for it.
    pub(crate) fn push_constant(&mut self, value: f64) {
        for i in 0..self.dim.size() {
            self.buffers[i].push(value);
        }
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Panics
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub(crate) fn push_point(&mut self, point: &impl PointTrait<T = f64>) {
        self.try_push_point(point).unwrap()
    }

    /// Push a new point onto the end of this coordinate buffer
    ///
    /// ## Errors
    ///
    /// - If the added point does not have the same dimension as the coordinate buffer.
    pub(crate) fn try_push_point(
        &mut self,
        point: &impl PointTrait<T = f64>,
    ) -> GeoArrowResult<()> {
        if let Some(coord) = point.coord() {
            self.try_push_coord(&coord)?;
        } else {
            self.push_constant(f64::NAN);
        };
        Ok(())
    }

    /// Construct a new builder and pre-fill it with coordinates from the provided iterator
    pub fn from_coords<'a>(
        coords: impl ExactSizeIterator<Item = &'a (impl CoordTrait<T = f64> + 'a)>,
        dim: Dimension,
    ) -> GeoArrowResult<Self> {
        let mut buffer = SeparatedCoordBufferBuilder::with_capacity(coords.len(), dim);
        for coord in coords {
            buffer.try_push_coord(coord)?;
        }
        Ok(buffer)
    }

    /// Consume the builder and convert to an immutable [`SeparatedCoordBuffer`]
    pub fn finish(self) -> SeparatedCoordBuffer {
        // Initialize buffers with empty array, then mutate into it
        let mut buffers = core::array::from_fn(|_| vec![].into());
        for (i, buffer) in self.buffers.into_iter().enumerate() {
            buffers[i] = buffer.into();
        }
        SeparatedCoordBuffer::from_array(buffers, self.dim).unwrap()
    }
}

// #[cfg(test)]
// mod test {
//     use wkt::types::Coord;

//     use super::*;

//     #[test]
//     fn errors_when_pushing_incompatible_coord() {
//         let mut builder = SeparatedCoordBufferBuilder::new(Dimension::XY);
//         builder
//             .try_push_coord(&Coord {
//                 x: 0.0,
//                 y: 0.0,
//                 z: Some(0.0),
//                 m: None,
//             })
//             .expect_err("Should err pushing XYZ to XY buffer");

//         let mut builder = SeparatedCoordBufferBuilder::new(Dimension::XYZ);
//         builder
//             .try_push_coord(&Coord {
//                 x: 0.0,
//                 y: 0.0,
//                 z: None,
//                 m: None,
//             })
//             .expect_err("Should err pushing XY to XYZ buffer");
//         builder
//             .try_push_coord(&Coord {
//                 x: 0.0,
//                 y: 0.0,
//                 z: Some(0.0),
//                 m: None,
//             })
//             .unwrap();
//     }
// }
