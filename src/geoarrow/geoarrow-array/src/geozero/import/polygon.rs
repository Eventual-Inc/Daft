use geoarrow_schema::PolygonType;
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    array::PolygonArray,
    builder::PolygonBuilder,
    capacity::PolygonCapacity,
    geozero::import::util::{from_xy, from_xyzm},
};

/// GeoZero trait to convert to GeoArrow PolygonArray.
pub trait ToPolygonArray {
    /// Convert to GeoArrow PolygonArray
    fn to_polygon_array(&self, typ: PolygonType) -> geozero::error::Result<PolygonArray> {
        Ok(self.to_polygon_builder(typ)?.finish())
    }

    /// Convert to a GeoArrow PolygonBuilder
    fn to_polygon_builder(&self, typ: PolygonType) -> geozero::error::Result<PolygonBuilder>;
}

impl<T: GeozeroGeometry> ToPolygonArray for T {
    fn to_polygon_builder(&self, typ: PolygonType) -> geozero::error::Result<PolygonBuilder> {
        let mut mutable_array = PolygonBuilder::new(typ);
        self.process_geom(&mut mutable_array)?;
        Ok(mutable_array)
    }
}

#[allow(unused_variables)]
impl GeomProcessor for PolygonBuilder {
    fn geometrycollection_begin(&mut self, size: usize, idx: usize) -> geozero::error::Result<()> {
        // reserve `size` geometries
        let capacity = PolygonCapacity::new(0, 0, size);
        self.reserve(capacity);
        Ok(())
    }

    fn geometrycollection_end(&mut self, idx: usize) -> geozero::error::Result<()> {
        // self.shrink_to_fit()
        Ok(())
    }

    fn xy(&mut self, x: f64, y: f64, idx: usize) -> geozero::error::Result<()> {
        // # Safety:
        // This upholds invariants because we call try_push_length in polygon_begin to ensure
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
        // This upholds invariants because we call try_push_length in polygon_begin to ensure
        // offset arrays are correct.
        self.push_coord(&from_xyzm(x, y, z, m).expect("valid coord"))
            .unwrap();
        Ok(())
    }

    // Here, size is the number of rings in the polygon
    fn polygon_begin(
        &mut self,
        tagged: bool,
        size: usize,
        idx: usize,
    ) -> geozero::error::Result<()> {
        // reserve `size` rings
        let capacity = PolygonCapacity::new(0, size, 0);
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
        // reserve `size` coordinates
        let capacity = PolygonCapacity::new(size, 0, 0);
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
//     use crate::test::polygon::{p0, p1};

//     #[test]
//     fn from_geozero() -> Result<()> {
//         let geo_geoms = vec![p0(), p1()];
//         let gc = Geometry::GeometryCollection(
//             geo_geoms
//                 .clone()
//                 .into_iter()
//                 .map(Geometry::Polygon)
//                 .collect(),
//         );
//         let typ = PolygonType::new(Dimension::XY, Default::default());
//         let geo_arr = gc.to_polygon_array(typ.clone()).unwrap();

//         let geo_arr2 = PolygonBuilder::from_polygons(&geo_geoms, typ).finish();

//         // These are constructed with two different code paths
//         assert_eq!(geo_arr, geo_arr2);
//         Ok(())
//     }
// }
