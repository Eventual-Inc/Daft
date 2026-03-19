use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor, array::MultiLineStringArray,
    geozero::export::scalar::process_multi_line_string,
};

impl GeozeroGeometry for MultiLineStringArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_multi_line_string(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::{Dimension, MultiLineStringType};
//     use geozero::ToWkt;

//     use crate::{
//         builder::MultiLineStringBuilder,
//         test::multilinestring::{ml0, ml1},
//     };

//     #[test]
//     fn geozero_process_geom() -> geozero::error::Result<()> {
//         let typ = MultiLineStringType::new(Dimension::XY, Default::default());
//         let geo_arr =
//             MultiLineStringBuilder::from_multi_line_strings(&[&ml0(), &ml1()], typ).finish();
//         let wkt = ToWkt::to_wkt(&geo_arr)?;
//         let expected = "GEOMETRYCOLLECTION(MULTILINESTRING((-111 45,-111 41,-104 41,-104 45)),MULTILINESTRING((-111 45,-111 41,-104 41,-104 45),(-110 44,-110 42,-105 42,-105 44)))";
//         assert_eq!(wkt, expected);
//         Ok(())
//     }
// }
