use geo_traits::MultiPointTrait;
use geozero::{GeomProcessor, GeozeroGeometry};

use super::process_point_as_coord;
use crate::scalar::MultiPoint;

pub(crate) fn process_multi_point<P: GeomProcessor>(
    geom: &impl MultiPointTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multipoint_begin(geom.num_points(), geom_idx)?;

    for (point_idx, point) in geom.points().enumerate() {
        process_point_as_coord(&point, point_idx, processor)?;
    }

    processor.multipoint_end(geom_idx)?;
    Ok(())
}

impl GeozeroGeometry for MultiPoint<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_point(self, 0, processor)
    }
}
