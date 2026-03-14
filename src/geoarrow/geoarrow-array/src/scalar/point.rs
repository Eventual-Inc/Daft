use geo_traits::PointTrait;
use geoarrow_schema::Dimension;

use crate::{array::CoordBuffer, eq::point_eq, scalar::Coord};

/// An Arrow equivalent of a Point
///
/// This implements [PointTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Point<'a> {
    coords: &'a CoordBuffer,
    geom_index: usize,
}

impl<'a> Point<'a> {
    pub(crate) fn new(coords: &'a CoordBuffer, geom_index: usize) -> Self {
        Point { coords, geom_index }
    }

    pub(crate) fn native_dim(&self) -> Dimension {
        self.coords.dim()
    }
}

impl<'a> PointTrait for Point<'a> {
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() { None } else { Some(coord) }
    }
}

impl<'a> PointTrait for &Point<'a> {
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() { None } else { Some(coord) }
    }
}

impl<G: PointTrait<T = f64>> PartialEq<G> for Point<'_> {
    fn eq(&self, other: &G) -> bool {
        point_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use crate::array::{CoordBuffer, PointArray};
//     use crate::trait_::ArrayAccessor;

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let x1 = vec![0., 1., 2.];
//         let y1 = vec![3., 4., 5.];
//         let buf1 = CoordBuffer::Separated((x1, y1).try_into().unwrap());
//         let arr1 = PointArray::new(buf1, None, Default::default());

//         let x2 = vec![0., 100., 2.];
//         let y2 = vec![3., 400., 5.];
//         let buf2 = CoordBuffer::Separated((x2, y2).try_into().unwrap());
//         let arr2 = PointArray::new(buf2, None, Default::default());

//         assert_eq!(arr1.value(0), arr2.value(0));
//     }
// }
