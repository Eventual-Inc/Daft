//! Scalar references onto a parent GeoArrow array.
//!
//! For all "native" GeoArrow scalar types, (all types defined in this module) it is `O(1)` and
//! allocation-free for any coordinate access.
//!
//! For "serialized" scalars emitted from the [`GenericWkbArray`][crate::array::GenericWkbArray],
//! [`WkbViewArray`][crate::array::WkbViewArray],
//! [`GenericWktArray`][crate::array::GenericWktArray], and
//! [`WktViewArray`][crate::array::WktViewArray], there is an initial parsing step when accessing
//! the scalar from the [`GeoArrowArrayAccessor`][crate::GeoArrowArrayAccessor] trait.
//!
//! All scalars implement [`geo_traits`]. You can iterate through geometry parts directly using the
//! APIs exposed by [`geo_traits`]. Or, for simplicity at the cost of a memory copy, you can use
//! the traits defined in [`geo_traits::to_geo`] to convert these scalars to [`geo_types`] objects
//! (though keep in mind ).
//!
//! ## Converting to [`geo_types`]
//!
//! You can convert these scalars to [`geo_types`] objects using the [`geo_traits::to_geo`] traits.
//!
//! There are a couple drawbacks:
//!
//! - `geo_types` only supports 2D geometries. Any other dimensions will be dropped.
//! - `geo_types` doesn't support empty points. This is why both
//!   [`ToGeoGeometry::to_geometry`][geo_traits::to_geo::ToGeoGeometry::to_geometry] and
//!   [`ToGeoGeometry::try_to_geometry`][geo_traits::to_geo::ToGeoGeometry::try_to_geometry] exist.
//!   The former will panic on any empty points.
//!

mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
mod specialization;

pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::Geometry;
pub use geometrycollection::GeometryCollection;
pub use linestring::LineString;
pub use multilinestring::MultiLineString;
pub use multipoint::MultiPoint;
pub use multipolygon::MultiPolygon;
pub use point::Point;
pub use polygon::Polygon;
pub use rect::Rect;
