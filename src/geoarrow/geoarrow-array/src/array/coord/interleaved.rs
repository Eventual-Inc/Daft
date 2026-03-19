use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field};
use geo_traits::CoordTrait;
use geoarrow_schema::{
    CoordType, Dimension, PointType,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::{builder::InterleavedCoordBufferBuilder, scalar::InterleavedCoord};

/// An array of coordinates stored interleaved in a single buffer.
///
/// This stores all coordinates in interleaved fashion in a single underlying buffer: e.g. `xyxyxy`
/// for 2D coordinates.
#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoordBuffer {
    pub(crate) coords: ScalarBuffer<f64>,
    pub(crate) dim: Dimension,
}

fn check(coords: &ScalarBuffer<f64>, dim: Dimension) -> GeoArrowResult<()> {
    if !coords.len().is_multiple_of(dim.size()) {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Length of interleaved coordinate buffer must be a multiple of the dimension size"
                .to_string(),
        ));
    }

    Ok(())
}

impl InterleavedCoordBuffer {
    /// The underlying coordinate type
    pub const COORD_TYPE: CoordType = CoordType::Interleaved;

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Panics
    ///
    /// - if coords.len() % dim.size() != 0
    pub fn new(coords: ScalarBuffer<f64>, dim: Dimension) -> Self {
        Self::try_new(coords, dim).unwrap()
    }

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the coordinate buffer have different lengths
    pub fn try_new(coords: ScalarBuffer<f64>, dim: Dimension) -> GeoArrowResult<Self> {
        check(&coords, dim)?;
        Ok(Self { coords, dim })
    }

    /// Construct from an iterator of coordinates.
    pub fn from_coords<'a>(
        coords: impl ExactSizeIterator<Item = &'a (impl CoordTrait<T = f64> + 'a)>,
        dim: Dimension,
    ) -> GeoArrowResult<Self> {
        Ok(InterleavedCoordBufferBuilder::from_coords(coords, dim)?.finish())
    }

    /// Access the underlying coordinate buffer.
    pub fn coords(&self) -> &ScalarBuffer<f64> {
        &self.coords
    }

    pub(crate) fn values_array(&self) -> Float64Array {
        Float64Array::new(self.coords.clone(), None)
    }

    /// The dimension of this coordinate buffer
    pub fn dim(&self) -> Dimension {
        self.dim
    }

    pub(crate) fn values_field(&self) -> Field {
        match self.dim {
            Dimension::XY => Field::new("xy", DataType::Float64, false),
            Dimension::XYZ => Field::new("xyz", DataType::Float64, false),
            Dimension::XYM => Field::new("xym", DataType::Float64, false),
            Dimension::XYZM => Field::new("xyzm", DataType::Float64, false),
        }
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coords: self
                .coords
                .slice(offset * self.dim.size(), length * self.dim.size()),
            dim: self.dim,
        }
    }

    pub(crate) fn storage_type(&self) -> DataType {
        PointType::new(self.dim, Default::default())
            .with_coord_type(Self::COORD_TYPE)
            .data_type()
    }

    /// The number of coordinates
    pub fn len(&self) -> usize {
        self.coords.len() / self.dim.size()
    }

    /// Whether this buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::CoordTrait;
    /// use geoarrow_array::array::InterleavedCoordBuffer;
    /// use geoarrow_schema::Dimension;
    ///
    /// let coords = [
    ///     geo_types::coord! { x: 1.0, y: 2.0 },
    ///     geo_types::coord! { x: 3.0, y: 4.0 },
    /// ];
    /// let coord_buffer = InterleavedCoordBuffer::from_coords(coords.iter(), Dimension::XY).unwrap();
    /// let coord = coord_buffer.value(0);
    /// assert_eq!(coord.x(), 1.0);
    /// assert_eq!(coord.y(), 2.0);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the value is outside the bounds of the buffer.
    pub fn value(&self, index: usize) -> InterleavedCoord<'_> {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::CoordTrait;
    /// use geoarrow_array::array::InterleavedCoordBuffer;
    /// use geoarrow_schema::Dimension;
    ///
    /// let coords = [
    ///     geo_types::coord! { x: 1.0, y: 2.0 },
    ///     geo_types::coord! { x: 3.0, y: 4.0 },
    /// ];
    /// let coord_buffer = InterleavedCoordBuffer::from_coords(coords.iter(), Dimension::XY).unwrap();
    /// let coord = unsafe { coord_buffer.value_unchecked(0) };
    /// assert_eq!(coord.x(), 1.0);
    /// assert_eq!(coord.y(), 2.0);
    /// ```
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the buffer.
    pub unsafe fn value_unchecked(&self, index: usize) -> InterleavedCoord<'_> {
        InterleavedCoord {
            coords: &self.coords,
            i: index,
            dim: self.dim,
        }
    }

    pub(crate) fn from_arrow(array: &FixedSizeListArray, dim: Dimension) -> GeoArrowResult<Self> {
        if array.value_length() != dim.size() as i32 {
            return Err(GeoArrowError::InvalidGeoArrow(format!(
                "Expected the FixedSizeListArray to match the dimension. Array length is {}, dimension is: {:?} have size 2",
                array.value_length(),
                dim
            )));
        }

        let coord_array_values = array
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        Ok(InterleavedCoordBuffer::new(
            coord_array_values.values().clone(),
            dim,
        ))
    }
}

impl From<InterleavedCoordBuffer> for FixedSizeListArray {
    fn from(value: InterleavedCoordBuffer) -> Self {
        FixedSizeListArray::new(
            Arc::new(value.values_field()),
            value.dim.size() as i32,
            Arc::new(value.values_array()),
            None,
        )
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn test_eq_slicing() {
//         let coords1 = vec![0., 3., 1., 4., 2., 5.];
//         let buf1 = InterleavedCoordBuffer::new(coords1.into(), Dimension::XY).slice(1, 1);

//         let coords2 = vec![1., 4.];
//         let buf2 = InterleavedCoordBuffer::new(coords2.into(), Dimension::XY);

//         assert_eq!(buf1, buf2);
//     }
// }
