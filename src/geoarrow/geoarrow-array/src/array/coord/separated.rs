use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, StructArray, cast::AsArray, types::Float64Type};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field};
use geo_traits::CoordTrait;
use geoarrow_schema::{
    CoordType, Dimension, PointType,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::{builder::SeparatedCoordBufferBuilder, scalar::SeparatedCoord};

/// An array of coordinates stored in separate buffers of the same length.
///
/// This stores all coordinates in separated fashion as multiple underlying buffers: e.g. `xxx` and
/// `yyy` for 2D coordinates.
#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer {
    /// We always store a buffer for all 4 dimensions. The buffers for dimension 3 and 4 may be
    /// empty.
    pub(crate) buffers: [ScalarBuffer<f64>; 4],
    pub(crate) dim: Dimension,
}

fn check(buffers: &[ScalarBuffer<f64>; 4], dim: Dimension) -> GeoArrowResult<()> {
    let all_same_length = match dim {
        Dimension::XY => buffers[0].len() == buffers[1].len(),
        Dimension::XYZ | Dimension::XYM => {
            buffers[0].len() == buffers[1].len() && buffers[1].len() == buffers[2].len()
        }
        Dimension::XYZM => {
            buffers[0].len() == buffers[1].len()
                && buffers[1].len() == buffers[2].len()
                && buffers[2].len() == buffers[3].len()
        }
    };

    if !all_same_length {
        return Err(GeoArrowError::InvalidGeoArrow(
            "all buffers must have the same length".to_string(),
        ));
    }

    Ok(())
}

impl SeparatedCoordBuffer {
    /// The underlying coordinate type
    pub const COORD_TYPE: CoordType = CoordType::Separated;

    /// Construct a new SeparatedCoordBuffer from an array of existing buffers.
    ///
    /// The number of _valid_ buffers in the array must match the dimension size. E.g. if the `dim`
    /// is `Dimension::XY`, then only the first two buffers must have non-zero length, and the last
    /// two buffers in the array can have length zero.
    pub fn from_array(buffers: [ScalarBuffer<f64>; 4], dim: Dimension) -> GeoArrowResult<Self> {
        check(&buffers, dim)?;
        Ok(Self { buffers, dim })
    }

    /// Construct a new SeparatedCoordBuffer from a `Vec` of existing buffers.
    ///
    /// All buffers within `buffers` must have the same length, and the length of `buffers` must
    /// equal the dimension size.
    pub fn from_vec(buffers: Vec<ScalarBuffer<f64>>, dim: Dimension) -> GeoArrowResult<Self> {
        if buffers.len() != dim.size() {
            return Err(GeoArrowError::InvalidGeoArrow(
                "Buffers must match dimension length ".into(),
            ));
        }

        let mut buffers = buffers.into_iter().map(Some).collect::<Vec<_>>();

        // Fill buffers with empty buffers past needed dimensions
        let buffers = core::array::from_fn(|i| {
            if i < buffers.len() {
                buffers[i].take().unwrap()
            } else {
                Vec::new().into()
            }
        });

        Self::from_array(buffers, dim)
    }

    /// Access the underlying coordinate buffers.
    ///
    /// Note that not all four buffers may be valid. Only so many buffers have defined meaning as
    /// there are dimensions, so for an XY buffer, only the first two buffers have defined meaning,
    /// and the last two may be any buffer, or empty.
    pub fn raw_buffers(&self) -> &[ScalarBuffer<f64>; 4] {
        &self.buffers
    }

    /// Access the underlying coordinate buffers.
    ///
    /// In comparison to raw_buffers, all of the returned buffers are valid.
    pub fn buffers(&self) -> Vec<ScalarBuffer<f64>> {
        match self.dim {
            Dimension::XY => {
                vec![self.buffers[0].clone(), self.buffers[1].clone()]
            }
            Dimension::XYZ | Dimension::XYM => {
                vec![
                    self.buffers[0].clone(),
                    self.buffers[1].clone(),
                    self.buffers[2].clone(),
                ]
            }
            Dimension::XYZM => {
                vec![
                    self.buffers[0].clone(),
                    self.buffers[1].clone(),
                    self.buffers[2].clone(),
                    self.buffers[3].clone(),
                ]
            }
        }
    }

    /// The dimension of this coordinate buffer
    pub fn dim(&self) -> Dimension {
        self.dim
    }

    pub(crate) fn values_array(&self) -> Vec<ArrayRef> {
        match self.dim {
            Dimension::XY => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                ]
            }
            Dimension::XYZ | Dimension::XYM => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[2].clone(), None)),
                ]
            }
            Dimension::XYZM => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[2].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[3].clone(), None)),
                ]
            }
        }
    }

    pub(crate) fn values_field(&self) -> Vec<Field> {
        match self.dim {
            Dimension::XY => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
            }
            Dimension::XYZ => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                ]
            }
            Dimension::XYM => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("m", DataType::Float64, false),
                ]
            }
            Dimension::XYZM => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                    Field::new("m", DataType::Float64, false),
                ]
            }
        }
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );

        // Initialize array with existing buffers, then overwrite them
        let mut sliced_buffers = self.buffers.clone();
        for (i, buffer) in self.buffers.iter().enumerate().take(self.dim.size()) {
            sliced_buffers[i] = buffer.slice(offset, length);
        }

        Self {
            buffers: sliced_buffers,
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
        self.buffers[0].len()
    }

    /// Whether the coordinate buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::CoordTrait;
    /// use geoarrow_array::array::SeparatedCoordBuffer;
    /// use geoarrow_schema::Dimension;
    ///
    /// let coords = [
    ///     geo_types::coord! { x: 1.0, y: 2.0 },
    ///     geo_types::coord! { x: 3.0, y: 4.0 },
    /// ];
    /// let coord_buffer = SeparatedCoordBuffer::from_coords(coords.iter(), Dimension::XY).unwrap();
    /// let coord = coord_buffer.value(0);
    /// assert_eq!(coord.x(), 1.0);
    /// assert_eq!(coord.y(), 2.0);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the value is outside the bounds of the buffer.
    pub fn value(&self, index: usize) -> SeparatedCoord<'_> {
        assert!(index <= self.len());
        unsafe { self.value_unchecked(index) }
    }

    /// Returns the element at index `i`, not considering validity.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo_traits::CoordTrait;
    /// use geoarrow_array::array::SeparatedCoordBuffer;
    /// use geoarrow_schema::Dimension;
    ///
    /// let coords = [
    ///     geo_types::coord! { x: 1.0, y: 2.0 },
    ///     geo_types::coord! { x: 3.0, y: 4.0 },
    /// ];
    /// let coord_buffer = SeparatedCoordBuffer::from_coords(coords.iter(), Dimension::XY).unwrap();
    /// let coord = unsafe { coord_buffer.value_unchecked(0) };
    /// assert_eq!(coord.x(), 1.0);
    /// assert_eq!(coord.y(), 2.0);
    /// ```
    ///
    /// # Safety
    ///
    /// Caller is responsible for ensuring that the index is within the bounds of the buffer.
    pub unsafe fn value_unchecked(&self, index: usize) -> SeparatedCoord<'_> {
        SeparatedCoord {
            buffers: &self.buffers,
            i: index,
            dim: self.dim,
        }
    }

    pub(crate) fn from_arrow(array: &StructArray, dim: Dimension) -> GeoArrowResult<Self> {
        let buffers = array
            .columns()
            .iter()
            .map(|c| c.as_primitive::<Float64Type>().values().clone())
            .collect();
        Self::from_vec(buffers, dim)
    }

    /// Construct from an iterator of coordinates
    pub fn from_coords<'a>(
        coords: impl ExactSizeIterator<Item = &'a (impl CoordTrait<T = f64> + 'a)>,
        dim: Dimension,
    ) -> GeoArrowResult<Self> {
        Ok(SeparatedCoordBufferBuilder::from_coords(coords, dim)?.finish())
    }
}

impl From<SeparatedCoordBuffer> for StructArray {
    fn from(value: SeparatedCoordBuffer) -> Self {
        StructArray::new(value.values_field().into(), value.values_array(), None)
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn test_eq_slicing() {
//         let x1 = vec![0., 1., 2.];
//         let y1 = vec![3., 4., 5.];

//         let buf1 = SeparatedCoordBuffer::from_vec(vec![x1.into(), y1.into()], Dimension::XY)
//             .unwrap()
//             .slice(1, 1);
//         dbg!(&buf1.buffers[0]);
//         dbg!(&buf1.buffers[1]);

//         let x2 = vec![1.];
//         let y2 = vec![4.];
//         let buf2 =
//             SeparatedCoordBuffer::from_vec(vec![x2.into(), y2.into()], Dimension::XY).unwrap();

//         assert_eq!(buf1, buf2);
//     }
// }
