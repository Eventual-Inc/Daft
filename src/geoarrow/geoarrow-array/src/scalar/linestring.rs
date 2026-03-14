use arrow_buffer::OffsetBuffer;
use geo_traits::LineStringTrait;
use geoarrow_schema::Dimension;

use crate::{array::CoordBuffer, eq::line_string_eq, scalar::Coord, util::OffsetBufferUtils};

/// An Arrow equivalent of a LineString
///
/// This implements [LineStringTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct LineString<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i64>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> LineString<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i64>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }

    pub(crate) fn native_dim(&self) -> Dimension {
        self.coords.dim()
    }
}

impl<'a> LineStringTrait for LineString<'a> {
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.coords.value(self.start_offset + i)
    }
}

impl<'a> LineStringTrait for &'a LineString<'a> {
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.coords.value(self.start_offset + i)
    }
}

impl<G: LineStringTrait<T = f64>> PartialEq<G> for LineString<'_> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
