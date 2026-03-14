use geo_traits::{GeometryTrait, GeometryType, UnimplementedLine, UnimplementedTriangle};

use crate::{eq::geometry_eq, scalar::*};

/// An Arrow equivalent of a Geometry
///
/// This implements [GeometryTrait], which you can use to extract data.
#[derive(Debug)]
pub enum Geometry<'a> {
    /// Point geometry
    Point(crate::scalar::Point<'a>),
    /// LineString geometry
    LineString(crate::scalar::LineString<'a>),
    /// Polygon geometry
    Polygon(crate::scalar::Polygon<'a>),
    /// MultiPoint geometry
    MultiPoint(crate::scalar::MultiPoint<'a>),
    /// MultiLineString geometry
    MultiLineString(crate::scalar::MultiLineString<'a>),
    /// MultiPolygon geometry
    MultiPolygon(crate::scalar::MultiPolygon<'a>),
    /// GeometryCollection geometry
    GeometryCollection(crate::scalar::GeometryCollection<'a>),
    /// Rect geometry
    Rect(crate::scalar::Rect<'a>),
}

impl GeometryTrait for Geometry<'_> {
    type T = f64;
    type PointType<'b>
        = Point<'b>
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString<'b>
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon<'b>
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint<'b>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString<'b>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon<'b>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection<'b>
    where
        Self: 'b;
    type RectType<'b>
        = Rect<'b>
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<f64>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<f64>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Geometry::Point(p) => p.dim(),
            Geometry::LineString(p) => p.dim(),
            Geometry::Polygon(p) => p.dim(),
            Geometry::MultiPoint(p) => p.dim(),
            Geometry::MultiLineString(p) => p.dim(),
            Geometry::MultiPolygon(p) => p.dim(),
            Geometry::GeometryCollection(p) => p.dim(),
            Geometry::Rect(p) => p.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        Self::PointType<'_>,
        Self::LineStringType<'_>,
        Self::PolygonType<'_>,
        Self::MultiPointType<'_>,
        Self::MultiLineStringType<'_>,
        Self::MultiPolygonType<'_>,
        Self::GeometryCollectionType<'_>,
        Self::RectType<'_>,
        Self::TriangleType<'_>,
        Self::LineType<'_>,
    > {
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Geometry::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl<'a> GeometryTrait for &'a Geometry<'a> {
    type T = f64;
    type PointType<'b>
        = Point<'b>
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString<'b>
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon<'b>
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint<'b>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString<'b>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon<'b>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection<'b>
    where
        Self: 'b;
    type RectType<'b>
        = Rect<'b>
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<f64>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<f64>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Geometry::Point(p) => p.dim(),
            Geometry::LineString(p) => p.dim(),
            Geometry::Polygon(p) => p.dim(),
            Geometry::MultiPoint(p) => p.dim(),
            Geometry::MultiLineString(p) => p.dim(),
            Geometry::MultiPolygon(p) => p.dim(),
            Geometry::GeometryCollection(p) => p.dim(),
            Geometry::Rect(p) => p.dim(),
        }
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
        '_,
        Self::PointType<'_>,
        Self::LineStringType<'_>,
        Self::PolygonType<'_>,
        Self::MultiPointType<'_>,
        Self::MultiLineStringType<'_>,
        Self::MultiPolygonType<'_>,
        Self::GeometryCollectionType<'_>,
        Self::RectType<'_>,
        Self::TriangleType<'_>,
        Self::LineType<'_>,
    > {
        match self {
            Geometry::Point(p) => GeometryType::Point(p),
            Geometry::LineString(p) => GeometryType::LineString(p),
            Geometry::Polygon(p) => GeometryType::Polygon(p),
            Geometry::MultiPoint(p) => GeometryType::MultiPoint(p),
            Geometry::MultiLineString(p) => GeometryType::MultiLineString(p),
            Geometry::MultiPolygon(p) => GeometryType::MultiPolygon(p),
            Geometry::GeometryCollection(p) => GeometryType::GeometryCollection(p),
            Geometry::Rect(p) => GeometryType::Rect(p),
        }
    }
}

impl<G: GeometryTrait<T = f64>> PartialEq<G> for Geometry<'_> {
    fn eq(&self, other: &G) -> bool {
        geometry_eq(self, other)
    }
}
