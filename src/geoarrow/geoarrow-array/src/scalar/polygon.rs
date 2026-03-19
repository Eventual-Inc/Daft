use arrow_buffer::OffsetBuffer;
use geo_traits::PolygonTrait;
use geoarrow_schema::Dimension;

use crate::{array::CoordBuffer, eq::polygon_eq, scalar::LineString, util::OffsetBufferUtils};

/// An Arrow equivalent of a Polygon
///
/// This implements [PolygonTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Polygon<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i64>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> Polygon<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i64>,
        ring_offsets: &'a OffsetBuffer<i64>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }

    pub(crate) fn native_dim(&self) -> Dimension {
        self.coords.dim()
    }
}

impl<'a> PolygonTrait for Polygon<'a> {
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        // Note: we need to use saturating_sub in the case of an empty polygon, where start == end
        (end - start).saturating_sub(1)
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl<'a> PolygonTrait for &'a Polygon<'a> {
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        // Note: we need to use saturating_sub in the case of an empty polygon, where start == end
        (end - start).saturating_sub(1)
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl<G: PolygonTrait<T = f64>> PartialEq<G> for Polygon<'_> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use geo::HasDimensions;
//     use geo_traits::to_geo::ToGeoPolygon;
//     use geoarrow_schema::{Dimension, PolygonType};
//     use wkt::wkt;

//     use crate::{GeoArrowArrayAccessor, builder::PolygonBuilder};

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_access_empty_polygon() {
//         let empty_polygon: wkt::types::Polygon<f64> = wkt! { POLYGON EMPTY };
//         let typ = PolygonType::new(Dimension::XY, Default::default());
//         let polygon_array = PolygonBuilder::from_polygons(&[empty_polygon], typ).finish();

//         let geo_polygon = polygon_array.value(0).unwrap().to_polygon();
//         assert!(geo_polygon.is_empty());
//     }
// }
