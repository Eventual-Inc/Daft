use arrow_array::OffsetSizeTrait;
use geozero::{GeomProcessor, GeozeroGeometry, error::GeozeroError};

use crate::{
    GeoArrowArray, GeoArrowArrayAccessor,
    array::{GenericWkbArray, WkbViewArray},
    geozero::export::scalar::process_geometry,
};

impl<O: OffsetSizeTrait> GeozeroGeometry for GenericWkbArray<O> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let geom = &self
                .value(geom_idx)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(geom, geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}

impl GeozeroGeometry for WkbViewArray {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        let num_geometries = self.len();
        processor.geometrycollection_begin(num_geometries, 0)?;

        for geom_idx in 0..num_geometries {
            let geom = &self
                .value(geom_idx)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(geom, geom_idx, processor)?;
        }

        processor.geometrycollection_end(num_geometries - 1)?;
        Ok(())
    }
}
