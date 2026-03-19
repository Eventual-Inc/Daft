use arrow_buffer::ScalarBuffer;
use geo_traits::CoordTrait;
use geoarrow_schema::Dimension;

use crate::{eq::coord_eq, scalar::InterleavedCoord};

/// An Arrow equivalent of a Coord
///
/// This implements [CoordTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct SeparatedCoord<'a> {
    pub(crate) buffers: &'a [ScalarBuffer<f64>; 4],
    pub(crate) i: usize,
    pub(crate) dim: Dimension,
}

impl SeparatedCoord<'_> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        (0..self.dim.size()).all(|coord_dim| self.nth_or_panic(coord_dim).is_nan())
    }
}

impl PartialEq for SeparatedCoord<'_> {
    fn eq(&self, other: &SeparatedCoord) -> bool {
        coord_eq(self, other)
    }
}

impl PartialEq<InterleavedCoord<'_>> for SeparatedCoord<'_> {
    fn eq(&self, other: &InterleavedCoord) -> bool {
        coord_eq(self, other)
    }
}

impl CoordTrait for SeparatedCoord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        self.buffers[n][self.i]
    }

    fn x(&self) -> Self::T {
        self.buffers[0][self.i]
    }

    fn y(&self) -> Self::T {
        self.buffers[1][self.i]
    }
}

impl CoordTrait for &SeparatedCoord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        self.buffers[n][self.i]
    }

    fn x(&self) -> Self::T {
        self.buffers[0][self.i]
    }

    fn y(&self) -> Self::T {
        self.buffers[1][self.i]
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::Dimension;

//     use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let x1 = vec![0., 1., 2.];
//         let y1 = vec![3., 4., 5.];
//         let buf1 =
//             SeparatedCoordBuffer::from_vec(vec![x1.into(), y1.into()], Dimension::XY).unwrap();
//         let coord1 = buf1.value(0);

//         let x2 = vec![0., 100., 2.];
//         let y2 = vec![3., 400., 5.];
//         let buf2 =
//             SeparatedCoordBuffer::from_vec(vec![x2.into(), y2.into()], Dimension::XY).unwrap();
//         let coord2 = buf2.value(0);

//         assert_eq!(coord1, coord2);
//     }

//     #[test]
//     fn test_eq_against_interleaved_coord() {
//         let x1 = vec![0., 1., 2.];
//         let y1 = vec![3., 4., 5.];
//         let buf1 =
//             SeparatedCoordBuffer::from_vec(vec![x1.into(), y1.into()], Dimension::XY).unwrap();
//         let coord1 = buf1.value(0);

//         let coords2 = vec![0., 3., 1., 4., 2., 5.];
//         let buf2 = InterleavedCoordBuffer::new(coords2.into(), Dimension::XY);
//         let coord2 = buf2.value(0);

//         assert_eq!(coord1, coord2);
//     }
// }
