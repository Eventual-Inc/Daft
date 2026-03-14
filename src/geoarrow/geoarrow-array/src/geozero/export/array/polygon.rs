use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PolygonArray,
    geozero::export::scalar::process_polygon,
};

impl GeozeroGeometry for PolygonArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_polygon(&self.value(geom_idx).unwrap(), true, geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::{Dimension, PolygonType};
//     use geozero::ToWkt;

//     use crate::{
//         builder::PolygonBuilder,
//         test::polygon::{p0, p1},
//     };

//     #[test]
//     fn geozero_process_geom() -> geozero::error::Result<()> {
//         let typ = PolygonType::new(Dimension::XY, Default::default());
//         let geo_arr = PolygonBuilder::from_polygons(&[&p0(), &p1()], typ).finish();
//         let wkt = ToWkt::to_wkt(&geo_arr)?;
//         let expected = "GEOMETRYCOLLECTION(POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45)),POLYGON((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44)))";
//         assert_eq!(wkt, expected);
//         Ok(())
//     }
// }
