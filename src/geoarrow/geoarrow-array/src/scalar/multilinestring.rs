use arrow_buffer::OffsetBuffer;
use geo_traits::MultiLineStringTrait;
use geoarrow_schema::Dimension;

use crate::{
    array::CoordBuffer, eq::multi_line_string_eq, scalar::LineString, util::OffsetBufferUtils,
};

/// An Arrow equivalent of a MultiLineString
///
/// This implements [MultiLineStringTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct MultiLineString<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i64>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> MultiLineString<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i64>,
        ring_offsets: &'a OffsetBuffer<i64>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }

    pub(crate) fn native_dim(&self) -> Dimension {
        self.coords.dim()
    }
}

impl<'a> MultiLineStringTrait for MultiLineString<'a> {
    type InnerLineStringType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::InnerLineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<'a> MultiLineStringTrait for &'a MultiLineString<'a> {
    type InnerLineStringType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::InnerLineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for MultiLineString<'_> {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::{Dimension, MultiLineStringType};

//     use crate::{
//         builder::MultiLineStringBuilder,
//         test::multilinestring::{ml0, ml1},
//         trait_::GeoArrowArrayAccessor,
//     };

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let typ = MultiLineStringType::new(Dimension::XY, Default::default());

//         let arr1 = MultiLineStringBuilder::from_multi_line_strings(
//             vec![ml0(), ml1()].as_slice(),
//             typ.clone(),
//         )
//         .finish();
//         let arr2 =
//             MultiLineStringBuilder::from_multi_line_strings(vec![ml0(), ml0()].as_slice(), typ)
//                 .finish();

//         assert_eq!(arr1.value(0).unwrap(), arr2.value(0).unwrap());
//         assert_ne!(arr1.value(1).unwrap(), arr2.value(1).unwrap());
//     }
// }
