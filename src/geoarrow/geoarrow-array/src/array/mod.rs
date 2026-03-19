//! The concrete array definitions.
//!
//! All arrays implement the core [GeoArrowArray] trait.

mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod mixed;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
mod wkb;
mod wkb_view;
mod wkt;
mod wkt_view;

use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;
pub use coord::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use geoarrow_schema::{GeoArrowType, error::GeoArrowResult};
pub(crate) use geometry::DimensionIndex;
pub use geometry::GeometryArray;
pub use geometrycollection::GeometryCollectionArray;
pub use linestring::LineStringArray;
pub(crate) use mixed::MixedGeometryArray;
pub use multilinestring::MultiLineStringArray;
pub use multipoint::MultiPointArray;
pub use multipolygon::MultiPolygonArray;
pub use point::PointArray;
pub use polygon::PolygonArray;
pub use rect::RectArray;
pub use wkb::{GenericWkbArray, LargeWkbArray, WkbArray};
pub use wkb_view::WkbViewArray;
pub use wkt::{GenericWktArray, LargeWktArray, WktArray};
pub use wkt_view::WktViewArray;

use crate::GeoArrowArray;

/// Construct a new [GeoArrowArray] from an Arrow [Array] and [Field].
pub fn from_arrow_array(
    array: &dyn Array,
    field: &Field,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;

    let geo_type = GeoArrowType::from_arrow_field(field)?;
    let result: Arc<dyn GeoArrowArray> = match geo_type {
        Point(_) => Arc::new(PointArray::try_from((array, field))?),
        LineString(_) => Arc::new(LineStringArray::try_from((array, field))?),
        Polygon(_) => Arc::new(PolygonArray::try_from((array, field))?),
        MultiPoint(_) => Arc::new(MultiPointArray::try_from((array, field))?),
        MultiLineString(_) => Arc::new(MultiLineStringArray::try_from((array, field))?),
        MultiPolygon(_) => Arc::new(MultiPolygonArray::try_from((array, field))?),
        GeometryCollection(_) => Arc::new(GeometryCollectionArray::try_from((array, field))?),
        Rect(_) => Arc::new(RectArray::try_from((array, field))?),
        Geometry(_) => Arc::new(GeometryArray::try_from((array, field))?),
        Wkb(_) => Arc::new(WkbArray::try_from((array, field))?),
        LargeWkb(_) => Arc::new(LargeWkbArray::try_from((array, field))?),
        WkbView(_) => Arc::new(WkbViewArray::try_from((array, field))?),
        Wkt(_) => Arc::new(WktArray::try_from((array, field))?),
        LargeWkt(_) => Arc::new(LargeWktArray::try_from((array, field))?),
        WktView(_) => Arc::new(WktViewArray::try_from((array, field))?),
    };
    Ok(result)
}

// TODO: should we have an API to get the raw underlying string/&[u8] value?

/// A trait for GeoArrow arrays that can hold WKB data.
///
/// Currently three types are supported:
///
/// - [`GenericWkbArray<i32>`]
/// - [`GenericWkbArray<i64>`]
/// - [`WkbViewArray`]
///
/// This trait helps to abstract over the different types of WKB arrays so that we don’t need to
/// duplicate the implementation for each type.
///
/// This is modeled after the upstream [`BinaryArrayType`][arrow_array::array::BinaryArrayType]
/// trait.
pub trait GenericWkbArrayType<'a>:
    Sized + crate::GeoArrowArrayAccessor<'a, Item = ::wkb::reader::Wkb<'a>>
{
}

impl GenericWkbArrayType<'_> for GenericWkbArray<i32> {}
impl GenericWkbArrayType<'_> for GenericWkbArray<i64> {}
impl GenericWkbArrayType<'_> for WkbViewArray {}

/// A trait for GeoArrow arrays that can hold WKT data.
///
/// Currently three types are supported:
///
/// - [`GenericWktArray<i32>`]
/// - [`GenericWktArray<i64>`]
/// - [`WktViewArray`]
///
/// This trait helps to abstract over the different types of WKT arrays so that we don’t need to
/// duplicate the implementation for each type.
///
/// This is modeled after the upstream [`StringArrayType`][arrow_array::array::StringArrayType]
/// trait.
pub trait GenericWktArrayType:
    Sized + for<'a> crate::GeoArrowArrayAccessor<'a, Item = ::wkt::Wkt>
{
}

impl GenericWktArrayType for GenericWktArray<i32> {}
impl GenericWktArrayType for GenericWktArray<i64> {}
impl GenericWktArrayType for WktViewArray {}
