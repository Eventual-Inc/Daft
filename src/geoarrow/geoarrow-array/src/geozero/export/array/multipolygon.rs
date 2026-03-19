use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor, array::MultiPolygonArray,
    geozero::export::scalar::process_multi_polygon,
};

impl GeozeroGeometry for MultiPolygonArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_polygon(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::{Dimension, MultiPolygonType};
//     use geozero::ToWkt;

//     use crate::{
//         builder::MultiPolygonBuilder,
//         test::multipolygon::{mp0, mp1},
//     };

//     #[test]
//     fn geozero_process_geom() -> geozero::error::Result<()> {
//         let typ = MultiPolygonType::new(Dimension::XY, Default::default());
//         let geo_arr = MultiPolygonBuilder::from_multi_polygons(&[&mp0(), &mp1()], typ).finish();
//         let wkt = ToWkt::to_wkt(&geo_arr)?;
//         let expected = "GEOMETRYCOLLECTION(MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-111 45,-111 41,-104 41,-104 45,-111 45),(-110 44,-110 42,-105 42,-105 44,-110 44))),MULTIPOLYGON(((-111 45,-111 41,-104 41,-104 45,-111 45)),((-110 44,-110 42,-105 42,-105 44,-110 44))))";
//         assert_eq!(wkt, expected);
//         Ok(())
//     }
// }
