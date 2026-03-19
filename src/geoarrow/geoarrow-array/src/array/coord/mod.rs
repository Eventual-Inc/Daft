//! Contains implementations for how to encode arrays of coordinates for all other geometry array
//! types.
//!
//! Coordinates can be either _interleaved_, where they're represented as a `FixedSizeList`, or
//! _separated_, where they're represented with a `StructArray`.

mod combined;
mod interleaved;
mod separated;

pub use combined::CoordBuffer;
pub use interleaved::InterleavedCoordBuffer;
pub use separated::SeparatedCoordBuffer;
