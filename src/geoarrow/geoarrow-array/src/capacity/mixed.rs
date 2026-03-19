use std::ops::Add;

use geo_traits::*;
use geoarrow_schema::{
    Dimension,
    error::{GeoArrowError, GeoArrowResult},
};
use wkt::WktNum;

use crate::{
    builder::geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper},
    capacity::{
        LineStringCapacity, MultiLineStringCapacity, MultiPointCapacity, MultiPolygonCapacity,
        PolygonCapacity,
    },
};

/// A counter for the buffer sizes of a [`MixedGeometryArray`][crate::array::MixedGeometryArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Default, Debug, Clone, Copy)]
pub struct MixedCapacity {
    /// Simple: just the total number of points, nulls included
    pub(crate) point: usize,
    pub(crate) line_string: LineStringCapacity,
    pub(crate) polygon: PolygonCapacity,
    pub(crate) multi_point: MultiPointCapacity,
    pub(crate) multi_line_string: MultiLineStringCapacity,
    pub(crate) multi_polygon: MultiPolygonCapacity,
}

impl MixedCapacity {
    /// Create a new capacity with known sizes.
    pub(crate) fn new(
        point: usize,
        line_string: LineStringCapacity,
        polygon: PolygonCapacity,
        multi_point: MultiPointCapacity,
        multi_line_string: MultiLineStringCapacity,
        multi_polygon: MultiPolygonCapacity,
    ) -> Self {
        Self {
            point,
            line_string,
            polygon,
            multi_point,
            multi_line_string,
            multi_polygon,
        }
    }

    /// Create a new empty capacity.
    pub(crate) fn new_empty() -> Self {
        Self {
            point: 0,
            line_string: LineStringCapacity::new_empty(),
            polygon: PolygonCapacity::new_empty(),
            multi_point: MultiPointCapacity::new_empty(),
            multi_line_string: MultiLineStringCapacity::new_empty(),
            multi_polygon: MultiPolygonCapacity::new_empty(),
        }
    }

    /// Return `true` if the capacity is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.point == 0
            && self.line_string.is_empty()
            && self.polygon.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub(crate) fn total_num_geoms(&self) -> usize {
        let mut total = 0;
        total += self.point;
        total += self.line_string.geom_capacity();
        total += self.polygon.geom_capacity();
        total += self.multi_point.geom_capacity();
        total += self.multi_line_string.geom_capacity();
        total += self.multi_polygon.geom_capacity();
        total
    }

    #[inline]
    pub(crate) fn add_point(&mut self) {
        self.point += 1;
    }

    #[inline]
    pub(crate) fn add_line_string(&mut self, line_string: &impl LineStringTrait) {
        self.line_string.add_line_string(Some(line_string));
    }

    #[inline]
    pub(crate) fn add_polygon(&mut self, polygon: &impl PolygonTrait) {
        self.polygon.add_polygon(Some(polygon));
    }

    #[inline]
    pub(crate) fn add_multi_point(&mut self, multi_point: &impl MultiPointTrait) {
        self.multi_point.add_multi_point(Some(multi_point));
    }

    #[inline]
    pub(crate) fn add_multi_line_string(&mut self, multi_line_string: &impl MultiLineStringTrait) {
        self.multi_line_string
            .add_multi_line_string(Some(multi_line_string));
    }

    #[inline]
    pub(crate) fn add_multi_polygon(&mut self, multi_polygon: &impl MultiPolygonTrait) {
        self.multi_polygon.add_multi_polygon(Some(multi_polygon));
    }

    #[inline]
    pub(crate) fn add_geometry<T: WktNum>(
        &mut self,
        geom: &impl GeometryTrait<T = T>,
    ) -> GeoArrowResult<()> {
        match geom.as_type() {
            geo_traits::GeometryType::Point(_) => self.add_point(),
            geo_traits::GeometryType::LineString(g) => self.add_line_string(g),
            geo_traits::GeometryType::Polygon(g) => self.add_polygon(g),
            geo_traits::GeometryType::MultiPoint(p) => self.add_multi_point(p),
            geo_traits::GeometryType::MultiLineString(p) => self.add_multi_line_string(p),
            geo_traits::GeometryType::MultiPolygon(p) => self.add_multi_polygon(p),
            geo_traits::GeometryType::GeometryCollection(_) => {
                return Err(GeoArrowError::InvalidGeoArrow(
                    "nested geometry collections not supported in GeoArrow".to_string(),
                ));
            }
            geo_traits::GeometryType::Rect(r) => self.add_polygon(&RectWrapper::try_new(r)?),
            geo_traits::GeometryType::Triangle(tri) => self.add_polygon(&TriangleWrapper(tri)),
            geo_traits::GeometryType::Line(l) => self.add_line_string(&LineWrapper(l)),
        };
        Ok(())
    }

    /// The number of bytes an array with this capacity would occupy.
    pub(crate) fn num_bytes(&self, dim: Dimension) -> usize {
        let mut count = self.point * dim.size() * 8;
        count += self.line_string.num_bytes(dim);
        count += self.polygon.num_bytes(dim);
        count += self.multi_point.num_bytes(dim);
        count += self.multi_line_string.num_bytes(dim);
        count += self.multi_polygon.num_bytes(dim);
        count
    }
}

impl Add for MixedCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let point = self.point + rhs.point;
        let line_string = self.line_string + rhs.line_string;
        let polygon = self.polygon + rhs.polygon;
        let multi_point = self.multi_point + rhs.multi_point;
        let multi_line_string = self.multi_line_string + rhs.multi_line_string;
        let multi_polygon = self.multi_polygon + rhs.multi_polygon;

        Self::new(
            point,
            line_string,
            polygon,
            multi_point,
            multi_line_string,
            multi_polygon,
        )
    }
}
