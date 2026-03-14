//! Wrappers around `RectTrait`, `TriangleTrait`, and `LineTrait` to implement
//! `PolygonTrait`, `PolygonTrait` and `LineStringTrait` traits, respectively.
//!
//! This makes it easier to use `Rect`, `Triangle`, and `Line` types because we don't have to add
//! specialized code for them.

use geo_traits::{
    CoordTrait, GeometryTrait, LineStringTrait, LineTrait, PolygonTrait, RectTrait, TriangleTrait,
    UnimplementedGeometryCollection, UnimplementedLine, UnimplementedLineString,
    UnimplementedMultiLineString, UnimplementedMultiPoint, UnimplementedMultiPolygon,
    UnimplementedPoint, UnimplementedPolygon, UnimplementedRect, UnimplementedTriangle,
};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use wkt::WktNum;

pub(crate) struct RectWrapper<'a, T: WktNum, R: RectTrait<T = T>>(&'a R);

impl<'a, T: WktNum, R: RectTrait<T = T>> RectWrapper<'a, T, R> {
    pub(crate) fn try_new(rect: &'a R) -> GeoArrowResult<Self> {
        match rect.dim() {
            geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {}
            dim => {
                return Err(GeoArrowError::IncorrectGeometryType(format!(
                    "Only 2d rects supported when pushing to polygon. Got dimension: {dim:?}",
                )));
            }
        };

        Ok(Self(rect))
    }

    fn ll(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        wkt::types::Coord {
            x: lower.x(),
            y: lower.y(),
            z: None,
            m: None,
        }
    }

    fn ul(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        let upper = self.0.max();
        wkt::types::Coord {
            x: lower.x(),
            y: upper.y(),
            z: None,
            m: None,
        }
    }

    fn ur(&self) -> wkt::types::Coord<T> {
        let upper = self.0.max();
        wkt::types::Coord {
            x: upper.x(),
            y: upper.y(),
            z: None,
            m: None,
        }
    }

    fn lr(&self) -> wkt::types::Coord<T> {
        let lower = self.0.min();
        let upper = self.0.max();
        wkt::types::Coord {
            x: upper.x(),
            y: lower.y(),
            z: None,
            m: None,
        }
    }
}

impl<T: WktNum, R: RectTrait<T = T>> PolygonTrait for RectWrapper<'_, T, R> {
    type RingType<'a>
        = &'a RectWrapper<'a, T, R>
    where
        Self: 'a;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        Some(self)
    }

    fn num_interiors(&self) -> usize {
        0
    }

    unsafe fn interior_unchecked(&self, _: usize) -> Self::RingType<'_> {
        unreachable!("interior_unchecked called on a rect")
    }
}

impl<T: WktNum, R: RectTrait<T = T>> GeometryTrait for RectWrapper<'_, T, R> {
    type T = T;
    type PointType<'a>
        = UnimplementedPoint<T>
    where
        Self: 'a;
    type LineStringType<'a>
        = UnimplementedLineString<T>
    where
        Self: 'a;
    type PolygonType<'a>
        = RectWrapper<'a, T, R>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<T>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<T>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<T>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<T>
    where
        Self: 'a;
    type RectType<'a>
        = UnimplementedRect<T>
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<T>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<T>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
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
        geo_traits::GeometryType::Polygon(self)
    }
}

impl<'a, T: WktNum, R: RectTrait<T = T>> LineStringTrait for &'a RectWrapper<'a, T, R> {
    type CoordType<'b>
        = wkt::types::Coord<T>
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        5
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        // Ref below because I always forget the ordering
        // https://github.com/georust/geo/blob/76ad2a358bd079e9d47b1229af89608744d2635b/geo-types/src/geometry/rect.rs#L217-L225
        match i {
            0 => self.ll(),
            1 => self.ul(),
            2 => self.ur(),
            3 => self.lr(),
            4 => self.ll(),
            _ => unreachable!("out of range for rect coord: {i}"),
        }
    }
}

impl<'a, T: WktNum, R: RectTrait<T = T>> GeometryTrait for &'a RectWrapper<'a, T, R> {
    type T = T;
    type PointType<'b>
        = UnimplementedPoint<T>
    where
        Self: 'b;
    type LineStringType<'b>
        = UnimplementedLineString<T>
    where
        Self: 'b;
    type PolygonType<'b>
        = RectWrapper<'b, T, R>
    where
        Self: 'b;
    type MultiPointType<'b>
        = UnimplementedMultiPoint<T>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = UnimplementedMultiLineString<T>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = UnimplementedMultiPolygon<T>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = UnimplementedGeometryCollection<T>
    where
        Self: 'b;
    type RectType<'b>
        = UnimplementedRect<T>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<T>
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<T>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
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
        geo_traits::GeometryType::Polygon(self)
    }
}

pub(crate) struct TriangleWrapper<'a, T, Tri: TriangleTrait<T = T>>(pub(crate) &'a Tri);

impl<T, Tri: TriangleTrait<T = T>> PolygonTrait for TriangleWrapper<'_, T, Tri> {
    type RingType<'a>
        = &'a TriangleWrapper<'a, T, Tri>
    where
        Self: 'a;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        Some(self)
    }

    fn num_interiors(&self) -> usize {
        0
    }

    unsafe fn interior_unchecked(&self, _: usize) -> Self::RingType<'_> {
        unreachable!("interior_unchecked called on a triangle")
    }
}

impl<'a, T, Tri: TriangleTrait<T = T>> LineStringTrait for &'a TriangleWrapper<'a, T, Tri> {
    type CoordType<'b>
        = <Tri as TriangleTrait>::CoordType<'b>
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        4
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        match i {
            0 => self.0.first(),
            1 => self.0.second(),
            2 => self.0.third(),
            3 => self.0.first(),
            _ => unreachable!("out of range for triangle ring: {i}"),
        }
    }
}

impl<T, Tri: TriangleTrait<T = T>> GeometryTrait for TriangleWrapper<'_, T, Tri> {
    type T = T;
    type PointType<'a>
        = UnimplementedPoint<T>
    where
        Self: 'a;
    type LineStringType<'a>
        = UnimplementedLineString<T>
    where
        Self: 'a;
    type PolygonType<'a>
        = TriangleWrapper<'a, T, Tri>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<T>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<T>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<T>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<T>
    where
        Self: 'a;
    type RectType<'a>
        = UnimplementedRect<T>
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<T>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<T>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
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
        geo_traits::GeometryType::Polygon(self)
    }
}

impl<'a, T, Tri: TriangleTrait<T = T>> GeometryTrait for &'a TriangleWrapper<'a, T, Tri> {
    type T = T;
    type PointType<'b>
        = UnimplementedPoint<T>
    where
        Self: 'b;
    type LineStringType<'b>
        = UnimplementedLineString<T>
    where
        Self: 'b;
    type PolygonType<'b>
        = TriangleWrapper<'b, T, Tri>
    where
        Self: 'b;
    type MultiPointType<'b>
        = UnimplementedMultiPoint<T>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = UnimplementedMultiLineString<T>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = UnimplementedMultiPolygon<T>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = UnimplementedGeometryCollection<T>
    where
        Self: 'b;
    type RectType<'b>
        = UnimplementedRect<T>
    where
        Self: 'b;
    type TriangleType<'b>
        = UnimplementedTriangle<T>
    where
        Self: 'b;
    type LineType<'b>
        = UnimplementedLine<T>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
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
        geo_traits::GeometryType::Polygon(self)
    }
}

pub(crate) struct LineWrapper<'a, T, L: LineTrait<T = T>>(pub(crate) &'a L);

impl<T, L: LineTrait<T = T>> LineStringTrait for LineWrapper<'_, T, L> {
    type CoordType<'b>
        = <L as LineTrait>::CoordType<'b>
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        2
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        match i {
            0 => self.0.start(),
            1 => self.0.end(),
            _ => unreachable!("out of range for line coord: {i}"),
        }
    }
}

impl<T, L: LineTrait<T = T>> GeometryTrait for LineWrapper<'_, T, L> {
    type T = T;
    type PointType<'a>
        = UnimplementedPoint<T>
    where
        Self: 'a;
    type LineStringType<'a>
        = LineWrapper<'a, T, L>
    where
        Self: 'a;
    type PolygonType<'a>
        = UnimplementedPolygon<T>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<T>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<T>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<T>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<T>
    where
        Self: 'a;
    type RectType<'a>
        = UnimplementedRect<T>
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<T>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<T>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dim()
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
        geo_traits::GeometryType::LineString(self)
    }
}
