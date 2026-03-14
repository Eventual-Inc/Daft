use geoarrow_schema::MultiPolygonType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::MultiPolygonArray,
    builder::MultiPolygonBuilder,
    capacity::MultiPolygonCapacity,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow MultiPolygonArray.
pub trait ToMultiPolygonArray {
    /// Convert to GeoArrow MultiPolygonArray
    fn to_multi_polygon_array(
        &self,
        typ: MultiPolygonType,
    ) -> geozero::error::Result<MultiPolygonArray>;

    /// Convert to a GeoArrow MultiPolygonBuilder
    fn to_multi_polygon_builder(
        &self,
        typ: MultiPolygonType,
    ) -> geozero::error::Result<MultiPolygonBuilder>;
}

impl<T: GeozeroGeometry> ToMultiPolygonArray for T {
    fn to_multi_polygon_array(
        &self,
        typ: MultiPolygonType,
    ) -> geozero::error::Result<MultiPolygonArray> {
        Ok(self.to_multi_polygon_builder(typ)?.finish())
    }

    fn to_multi_polygon_builder(
        &self,
        typ: MultiPolygonType,
    ) -> geozero::error::Result<MultiPolygonBuilder> {
        let mut mutable_array = MultiPolygonBuilder::new(typ);
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for MultiPolygonBuilder {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` geometries
        let capacity = MultiPolygonCapacity::new(0, 0, 0, size);
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
        unsafe { self.push_coord(&from_xy(x, y).expect("valid coord")) }.unwrap();
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
        unsafe { self.push_coord(&from_xyzm(x, y, z, m).expect("valid coord")) }.unwrap();
        Ok(())
    }

    fn multipolygon_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` polygons
        let capacity = MultiPolygonCapacity::new(0, 0, size, 0);
        self.reserve(capacity);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
        self.try_push_geom_offset(size).unwrap();
        Ok(())
    }

    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // > An untagged Polygon is part of a MultiPolygon
        if tagged {
            // reserve 1 polygon
            let capacity = MultiPolygonCapacity::new(0, 0, 1, 0);
            self.reserve(capacity);

            // # Safety:
            // This upholds invariants because we separately update the ring offsets in
            // linestring_begin
            self.try_push_geom_offset(1).unwrap();
        }

        // reserve `size` rings
        let capacity = MultiPolygonCapacity::new(0, size, 0, 0);
        self.reserve(capacity);

        // # Safety:
        // This upholds invariants because we separately update the geometry offsets in
        // polygon_begin
        self.try_push_polygon_offset(size).unwrap();
        Ok(())
    }

    fn linestring_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        assert!(!tagged);

        // reserve `size` coordinates
        let capacity = MultiPolygonCapacity::new(size, 0, 0, 0);
        self.reserve(capacity);

        // # Safety:
        // This upholds invariants because we separately update the ring offsets in
        // linestring_begin
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
//     use crate::test::multipolygon::{mp0, mp1};

//     #[test]
//     fn from_geozero() -> Result<()> {
//         let geo_geoms = vec![mp0(), mp1()];

//         let geo = Geometry::GeometryCollection(
//             geo_geoms
//                 .clone()
//                 .into_iter()
//                 .map(Geometry::MultiPolygon)
//                 .collect(),
//         );
//         let typ = MultiPolygonType::new(Dimension::XY, Default::default());
//         let geo_arr = geo.to_multi_polygon_array(typ.clone()).unwrap();

//         let geo_arr2 = MultiPolygonBuilder::from_multi_polygons(&geo_geoms, typ).finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
