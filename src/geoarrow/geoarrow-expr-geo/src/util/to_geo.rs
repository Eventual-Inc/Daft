//! Convert structs that implement [geo_traits] to [geo] objects.
//!
//! Note that this is the same underlying implementation as upstream [geo] in
//! <https://github.com/georust/geo/pull/1255>. However, the trait-based implementation hits this
//! compiler regression <https://github.com/rust-lang/rust/issues/128887>,
//! <https://github.com/rust-lang/rust/issues/131960>, which prevents from compiling in release
//! mode on a stable Rust version. For some reason, the **function-based implementation** does not
//! hit this regression, and thus allows building geoarrow without using latest nightly and a
//! custom `RUSTFLAGS`.
//!
//! Note that it's only `GeometryTrait` and `GeometryCollectionTrait` that hit this compiler bug.
//! Other traits can use the upstream impls.

use geo::{CoordNum, Geometry, GeometryCollection};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType,
    to_geo::{
        ToGeoLine, ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon,
        ToGeoPoint, ToGeoPolygon, ToGeoRect, ToGeoTriangle,
    },
};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

/// Convert any [geo_traits] Geometry to a [`geo::Geometry`].
///
/// Only the first two dimensions will be kept.
pub fn geometry_to_geo<T: CoordNum>(
    geometry: &impl GeometryTrait<T = T>,
) -> GeoArrowResult<Geometry<T>> {
    use GeometryType::*;

    match geometry.as_type() {
        Point(geom) => Ok(Geometry::Point(geom.try_to_point().ok_or(
            GeoArrowError::IncorrectGeometryType(
                "geo crate does not support empty points.".to_string(),
            ),
        )?)),
        LineString(geom) => Ok(Geometry::LineString(geom.to_line_string())),
        Polygon(geom) => Ok(Geometry::Polygon(geom.to_polygon())),
        MultiPoint(geom) => Ok(Geometry::MultiPoint(geom.try_to_multi_point().ok_or(
            GeoArrowError::IncorrectGeometryType(
                "geo crate does not support empty points.".to_string(),
            ),
        )?)),
        MultiLineString(geom) => Ok(Geometry::MultiLineString(geom.to_multi_line_string())),
        MultiPolygon(geom) => Ok(Geometry::MultiPolygon(geom.to_multi_polygon())),
        GeometryCollection(geom) => Ok(Geometry::GeometryCollection(geometry_collection_to_geo(
            geom,
        )?)),
        Rect(geom) => Ok(Geometry::Rect(geom.to_rect())),
        Line(geom) => Ok(Geometry::Line(geom.to_line())),
        Triangle(geom) => Ok(Geometry::Triangle(geom.to_triangle())),
    }
}

/// Convert any GeometryCollection to a [`GeometryCollection`].
///
/// Only the first two dimensions will be kept.
fn geometry_collection_to_geo<T: CoordNum>(
    geometry_collection: &impl GeometryCollectionTrait<T = T>,
) -> GeoArrowResult<GeometryCollection<T>> {
    Ok(GeometryCollection::new_from(
        geometry_collection
            .geometries()
            .map(|geometry| geometry_to_geo(&geometry))
            .collect::<GeoArrowResult<_>>()?,
    ))
}
