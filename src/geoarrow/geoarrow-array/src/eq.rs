use std::fmt::Debug;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use num_traits::{Float, Num, NumCast};

// The same as geo-types::CoordFloat
pub trait CoordFloat: Num + Copy + NumCast + PartialOrd + Debug + Float {}
impl<T: Num + Copy + NumCast + PartialOrd + Debug + Float> CoordFloat for T {}

#[inline]
pub fn coord_eq<T: CoordFloat>(
    left: &impl CoordTrait<T = T>,
    right: &impl CoordTrait<T = T>,
) -> bool {
    let left_dim = left.dim();
    if left_dim != right.dim() {
        return false;
    }

    for i in 0..left_dim.size() {
        if left.nth_or_panic(i) != right.nth_or_panic(i) {
            return false;
        }
    }

    true
}

#[inline]
pub fn point_eq<T: CoordFloat>(
    left: &impl PointTrait<T = T>,
    right: &impl PointTrait<T = T>,
) -> bool {
    match (left.coord(), right.coord()) {
        (Some(left), Some(right)) => coord_eq(&left, &right),
        (None, None) => true,
        _ => false,
    }
}

#[inline]
pub fn line_string_eq<T: CoordFloat>(
    left: &impl LineStringTrait<T = T>,
    right: &impl LineStringTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_coords() != right.num_coords() {
        return false;
    }

    for (left_coord, right_coord) in left.coords().zip(right.coords()) {
        if !coord_eq(&left_coord, &right_coord) {
            return false;
        }
    }

    true
}

#[inline]
pub fn polygon_eq<T: CoordFloat>(
    left: &impl PolygonTrait<T = T>,
    right: &impl PolygonTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_interiors() != right.num_interiors() {
        return false;
    }

    match (left.exterior(), right.exterior()) {
        (None, None) => (),
        (Some(_), None) => {
            return false;
        }
        (None, Some(_)) => {
            return false;
        }
        (Some(left), Some(right)) => {
            if !line_string_eq(&left, &right) {
                return false;
            }
        }
    };

    for (left_interior, right_interior) in left.interiors().zip(right.interiors()) {
        if !line_string_eq(&left_interior, &right_interior) {
            return false;
        }
    }

    true
}

#[inline]
pub fn multi_point_eq<T: CoordFloat>(
    left: &impl MultiPointTrait<T = T>,
    right: &impl MultiPointTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_points() != right.num_points() {
        return false;
    }

    for (left_point, right_point) in left.points().zip(right.points()) {
        if !point_eq(&left_point, &right_point) {
            return false;
        }
    }

    true
}

#[inline]
pub fn multi_line_string_eq<T: CoordFloat>(
    left: &impl MultiLineStringTrait<T = T>,
    right: &impl MultiLineStringTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_line_strings() != right.num_line_strings() {
        return false;
    }

    for (left_line, right_line) in left.line_strings().zip(right.line_strings()) {
        if !line_string_eq(&left_line, &right_line) {
            return false;
        }
    }

    true
}

#[inline]
pub fn multi_polygon_eq<T: CoordFloat>(
    left: &impl MultiPolygonTrait<T = T>,
    right: &impl MultiPolygonTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_polygons() != right.num_polygons() {
        return false;
    }

    for (left_polygon, right_polygon) in left.polygons().zip(right.polygons()) {
        if !polygon_eq(&left_polygon, &right_polygon) {
            return false;
        }
    }

    true
}

#[inline]
pub fn rect_eq<T: CoordFloat>(left: &impl RectTrait<T = T>, right: &impl RectTrait<T = T>) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if !coord_eq(&left.min(), &right.min()) {
        return false;
    }

    if !coord_eq(&left.max(), &right.max()) {
        return false;
    }

    true
}

#[inline]
pub fn geometry_eq<T: CoordFloat>(
    left: &impl GeometryTrait<T = T>,
    right: &impl GeometryTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    match (left.as_type(), right.as_type()) {
        (GeometryType::Point(l), GeometryType::Point(r)) => {
            if !point_eq(l, r) {
                return false;
            }
        }
        (GeometryType::LineString(l), GeometryType::LineString(r)) => {
            if !line_string_eq(l, r) {
                return false;
            }
        }
        (GeometryType::Polygon(l), GeometryType::Polygon(r)) => {
            if !polygon_eq(l, r) {
                return false;
            }
        }
        (GeometryType::MultiPoint(l), GeometryType::MultiPoint(r)) => {
            if !multi_point_eq(l, r) {
                return false;
            }
        }
        (GeometryType::MultiLineString(l), GeometryType::MultiLineString(r)) => {
            if !multi_line_string_eq(l, r) {
                return false;
            }
        }
        (GeometryType::MultiPolygon(l), GeometryType::MultiPolygon(r)) => {
            if !multi_polygon_eq(l, r) {
                return false;
            }
        }
        (GeometryType::Rect(l), GeometryType::Rect(r)) => {
            if !rect_eq(l, r) {
                return false;
            }
        }
        (GeometryType::GeometryCollection(l), GeometryType::GeometryCollection(r)) => {
            if !geometry_collection_eq(l, r) {
                return false;
            }
        }
        _ => {
            return false;
        }
    }

    true
}

#[inline]
pub fn geometry_collection_eq<T: CoordFloat>(
    left: &impl GeometryCollectionTrait<T = T>,
    right: &impl GeometryCollectionTrait<T = T>,
) -> bool {
    if left.dim() != right.dim() {
        return false;
    }

    if left.num_geometries() != right.num_geometries() {
        return false;
    }

    for (left_geometry, right_geometry) in left.geometries().zip(right.geometries()) {
        if !geometry_eq(&left_geometry, &right_geometry) {
            return false;
        }
    }

    true
}

pub(crate) fn offset_buffer_eq<O: OffsetSizeTrait>(
    left: &OffsetBuffer<O>,
    right: &OffsetBuffer<O>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }

    for (o1, o2) in left.iter().zip(right.iter()) {
        if o1 != o2 {
            return false;
        }
    }

    true
}
