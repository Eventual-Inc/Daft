use geoarrow_schema::LineStringType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::LineStringArray,
    builder::LineStringBuilder,
    capacity::LineStringCapacity,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow LineStringArray.
pub trait ToLineStringArray {
    /// Convert to GeoArrow LineStringArray
    fn to_line_string_array(&self, typ: LineStringType) -> geozero::error::Result<LineStringArray> {
        Ok(self.to_line_string_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow LineStringBuilder
    fn to_line_string_builder(
        &self,
        typ: LineStringType,
    ) -> geozero::error::Result<LineStringBuilder>;
}

impl<T: GeozeroGeometry> ToLineStringArray for T {
    fn to_line_string_builder(
        &self,
        typ: LineStringType,
    ) -> geozero::error::Result<LineStringBuilder> {
        let mut mutable_array = LineStringBuilder::new(typ);
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for LineStringBuilder {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        let capacity = LineStringCapacity::new(0, size);
        self.reserve(capacity);
        Ok(())
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        // self.shrink_to_fit()
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        self.push_coord(&from_xy(x, y).expect("valid coord"))
            .unwrap();
        Ok(())
    }

    fn coordinate(
        &mut self,
        x: f64,
        y: f64,
        z: Option<f64>,
        m: Option<f64>,
        t: Option<f64>,
        tm: Option<u64>,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in multipoint_begin to ensure
        // offset arrays are correct.
        self.push_coord(&from_xyzm(x, y, z, m).expect("valid coord"))
            .unwrap();
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        let capacity = LineStringCapacity::new(size, 0);
        self.reserve(capacity);
        self.try_push_length(size).unwrap();
        Ok(())
    }

    fn linestring_end(&mut self, tagged: bool, idx: usize) -> geozero::error::Result<()> {
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geo_types::{Geometry, LineString};
//     use geoarrow_schema::Dimension;
//     use geozero::error::Result;

//     use super::*;
//     use crate::test::linestring::{ls0, ls1};

//     #[test]
//     fn from_geo_using_geozero() -> Result<()> {
//         let geo_geoms = vec![ls0(), LineString(vec![]), ls1()];
//         let geo = Geometry::GeometryCollection(
//             geo_geoms
//                 .clone()
//                 .into_iter()
//                 .map(Geometry::LineString)
//                 .collect(),
//         );
//         let typ = LineStringType::new(Dimension::XY, Default::default());
//         let geo_arr = geo.to_line_string_array(typ.clone()).unwrap();

//         let geo_arr2 = LineStringBuilder::from_line_strings(&geo_geoms, typ).finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
