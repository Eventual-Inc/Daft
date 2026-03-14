// Specialized implementations of GeometryTrait on each scalar type.

use geo_traits::GeometryTrait;

use crate::scalar::*;

macro_rules! impl_specialization {
    ($geometry_type:ident, $trait_name:ident) => {
        impl GeometryTrait for $geometry_type<'_> {
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
                = geo_traits::UnimplementedLine<f64>
            where
                Self: 'b;
            type TriangleType<'b>
                = geo_traits::UnimplementedTriangle<f64>
            where
                Self: 'b;

            fn dim(&self) -> geo_traits::Dimensions {
                self.native_dim().into()
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
                geo_traits::GeometryType::$geometry_type(self)
            }
        }

        impl GeometryTrait for &'_ $geometry_type<'_> {
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
                = geo_traits::UnimplementedLine<f64>
            where
                Self: 'b;
            type TriangleType<'b>
                = geo_traits::UnimplementedTriangle<f64>
            where
                Self: 'b;

            fn dim(&self) -> geo_traits::Dimensions {
                self.native_dim().into()
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
                geo_traits::GeometryType::$geometry_type(self)
            }
        }
    };
}

impl_specialization!(Point, PointTrait);
impl_specialization!(LineString, LineStringTrait);
impl_specialization!(Polygon, PolygonTrait);
impl_specialization!(MultiPoint, MultiPointTrait);
impl_specialization!(MultiLineString, MultiLineStringTrait);
impl_specialization!(MultiPolygon, MultiPolygonTrait);
impl_specialization!(GeometryCollection, GeometryCollectionTrait);
impl_specialization!(Rect, RectTrait);
