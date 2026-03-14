use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor, array::LineStringArray,
    geozero::export::scalar::process_line_string,
};

impl GeozeroGeometry for LineStringArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_line_string(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::CoordType;
//     use geozero::ToWkt;

//     use crate::test::linestring::ls_array;

//     #[test]
//     fn geozero_process_geom() -> geozero::error::Result<()> {
//         let arr = ls_array(CoordType::Interleaved);
//         let wkt = ToWkt::to_wkt(&arr)?;
//         let expected = "GEOMETRYCOLLECTION(LINESTRING(0 1,1 2),LINESTRING EMPTY,LINESTRING(3 4,5 6),LINESTRING EMPTY)";
//         assert_eq!(wkt, expected);
//         Ok(())
//     }
// }
