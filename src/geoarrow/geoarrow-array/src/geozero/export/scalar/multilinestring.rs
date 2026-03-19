use geo_traits::{LineStringTrait, MultiLineStringTrait};
use geozero::{GeomProcessor, GeozeroGeometry};

use super::process_coord;
use crate::scalar::MultiLineString;

pub(crate) fn process_multi_line_string<P: GeomProcessor>(
    geom: &impl MultiLineStringTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.multilinestring_begin(geom.num_line_strings(), geom_idx)?;

    for (line_idx, line) in geom.line_strings().enumerate() {
        processor.linestring_begin(false, line.num_coords(), line_idx)?;

        for (coord_idx, coord) in line.coords().enumerate() {
            process_coord(&coord, coord_idx, processor)?;
        }

        processor.linestring_end(false, line_idx)?;
    }

    processor.multilinestring_end(geom_idx)?;
    Ok(())
}

impl GeozeroGeometry for MultiLineString<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_multi_line_string(self, 0, processor)
    }
}
