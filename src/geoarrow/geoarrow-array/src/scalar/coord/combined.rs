use geo_traits::CoordTrait;

use crate::{
    eq::coord_eq,
    scalar::{InterleavedCoord, SeparatedCoord},
};

/// An Arrow equivalent of a Coord
///
/// This implements [CoordTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub enum Coord<'a> {
    /// Separated coordinate
    Separated(SeparatedCoord<'a>),
    /// Interleaved coordinate
    Interleaved(InterleavedCoord<'a>),
}

impl Coord<'_> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        match self {
            Coord::Separated(c) => c.is_nan(),
            Coord::Interleaved(c) => c.is_nan(),
        }
    }
}

impl PartialEq for Coord<'_> {
    fn eq(&self, other: &Self) -> bool {
        coord_eq(self, other)
    }
}

impl PartialEq<InterleavedCoord<'_>> for Coord<'_> {
    fn eq(&self, other: &InterleavedCoord<'_>) -> bool {
        coord_eq(self, other)
    }
}

impl PartialEq<SeparatedCoord<'_>> for Coord<'_> {
    fn eq(&self, other: &SeparatedCoord<'_>) -> bool {
        coord_eq(self, other)
    }
}

impl CoordTrait for Coord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Coord::Interleaved(c) => c.dim(),
            Coord::Separated(c) => c.dim(),
        }
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_or_panic(n),
            Coord::Separated(c) => c.nth_or_panic(n),
        }
    }

    fn x(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.x(),
            Coord::Separated(c) => c.x(),
        }
    }

    fn y(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.y(),
            Coord::Separated(c) => c.y(),
        }
    }
}

impl CoordTrait for &Coord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Coord::Interleaved(c) => c.dim(),
            Coord::Separated(c) => c.dim(),
        }
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_or_panic(n),
            Coord::Separated(c) => c.nth_or_panic(n),
        }
    }

    fn x(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.x(),
            Coord::Separated(c) => c.x(),
        }
    }

    fn y(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.y(),
            Coord::Separated(c) => c.y(),
        }
    }
}
