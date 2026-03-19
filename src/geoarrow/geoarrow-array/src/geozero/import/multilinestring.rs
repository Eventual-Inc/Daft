use geoarrow_schema::MultiLineStringType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::MultiLineStringArray,
    builder::MultiLineStringBuilder,
    capacity::MultiLineStringCapacity,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow MultiLineStringArray.
pub trait ToMultiLineStringArray {
    /// Convert to GeoArrow MultiLineStringArray
    fn to_multi_line_string_array(
        &self,
        typ: MultiLineStringType,
    ) -> geozero::error::Result<MultiLineStringArray> {
        Ok(self.to_multi_line_string_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow MultiLineStringBuilder
    fn to_multi_line_string_builder(
        &self,
        typ: MultiLineStringType,
    ) -> geozero::error::Result<MultiLineStringBuilder>;
}

impl<T: GeozeroGeometry> ToMultiLineStringArray for T {
    fn to_multi_line_string_builder(
        &self,
        typ: MultiLineStringType,
    ) -> geozero::error::Result<MultiLineStringBuilder> {
        let mut mutable_array = MultiLineStringBuilder::new(typ);
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for MultiLineStringBuilder {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` geometries
        let capacity = MultiLineStringCapacity::new(0, 0, size);
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

    // Here, size is the number of LineStrings in the MultiLineString
    fn multilinestring_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` line strings
        let capacity = MultiLineStringCapacity::new(0, size, 0);
        self.reserve(capacity);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
        self.try_push_geom_offset(size).unwrap();
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // > An untagged LineString is either a Polygon ring or part of a MultiLineString
        // So if tagged, we need to update the geometry offsets array.
        if tagged {
            // reserve 1 line strings
            let capacity = MultiLineStringCapacity::new(0, 1, 0);
            self.reserve(capacity);

            // # Safety:
            // This upholds invariants because we separately update the ring offsets in
            // linestring_begin
            self.try_push_geom_offset(1).unwrap();
        }

        // reserve `size` coordinates
        let capacity = MultiLineStringCapacity::new(size, 0, 0);
        self.reserve(capacity);

        // # Safety:
        // This upholds invariants because we separately update the geometry offsets in
        // polygon_begin
        self.try_push_ring_offset(size).unwrap();
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geo_types::Geometry;
//     use geoarrow_schema::Dimension;
//     use geozero::error::Result;

//     use super::*;
//     use crate::test::multilinestring::{ml0, ml1};

//     #[test]
//     fn from_geozero() -> Result<()> {
//         let geo_geoms = vec![ml0(), ml1()];

//         let geo = Geometry::GeometryCollection(
//             geo_geoms
//                 .clone()
//                 .into_iter()
//                 .map(Geometry::MultiLineString)
//                 .collect(),
//         );
//         let typ = MultiLineStringType::new(Dimension::XY, Default::default());
//         let geo_arr = geo.to_multi_line_string_array(typ.clone()).unwrap();

//         let geo_arr2 = MultiLineStringBuilder::from_multi_line_strings(&geo_geoms, typ).finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
