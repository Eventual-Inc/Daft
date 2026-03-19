use geo_traits::GeometryCollectionTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

use super::process_geometry;
use crate::scalar::GeometryCollection;

pub(crate) fn process_geometry_collection<P: GeomProcessor>(
    geom: &impl GeometryCollectionTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.geometrycollection_begin(geom.num_geometries(), geom_idx)?;

    for (i, geometry) in geom.geometries().enumerate() {
        process_geometry(&geometry, i, processor)?;
    }

    processor.geometrycollection_end(geom_idx)?;
    Ok(())
}

impl GeozeroGeometry for GeometryCollection<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_geometry_collection(&self, 0, processor)
    }
}
