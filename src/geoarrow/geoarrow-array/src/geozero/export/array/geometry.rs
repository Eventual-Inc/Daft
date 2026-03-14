use geozero::{GeomProcessor, GeozeroGeometry};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor, array::GeometryArray,
    geozero::export::scalar::process_geometry,
};

impl GeozeroGeometry for GeometryArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            process_geometry(&self.value(geom_idx).unwrap(), geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}
