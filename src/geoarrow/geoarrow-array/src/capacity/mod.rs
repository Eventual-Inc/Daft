//! Counters for managing buffer lengths for each geometry array type.
//!
//! The most memory-efficient way to construct an array from a set of geometries is to make a
//! first pass over these geometries to count exactly how big each underlying buffer of the Arrow
//! array must be, allocate _once_ for exactly what you need, and then fill those buffers in a
//! second pass. Capacity counters help with this process.

mod geometry;
mod geometrycollection;
mod linestring;
mod mixed;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod wkb;

pub use geometry::GeometryCapacity;
pub use geometrycollection::GeometryCollectionCapacity;
pub use linestring::LineStringCapacity;
pub(crate) use mixed::MixedCapacity;
pub use multilinestring::MultiLineStringCapacity;
pub use multipoint::MultiPointCapacity;
pub use multipolygon::MultiPolygonCapacity;
pub use polygon::PolygonCapacity;
pub use wkb::WkbCapacity;
