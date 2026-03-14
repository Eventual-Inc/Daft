use geo_traits::{CoordTrait, PointTrait};
use geozero::{GeomProcessor, GeozeroGeometry};

use crate::scalar::Point;

/// Process a [PointTrait] through a [GeomProcessor].
pub(crate) fn process_point<P: GeomProcessor>(
    geom: &impl PointTrait<T = f64>,
    geom_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    processor.point_begin(geom_idx)?;
    process_point_as_coord(geom, 0, processor)?;
    processor.point_end(geom_idx)?;
    Ok(())
}

/// Note that this does _not_ call `processor.point_begin` and `processor.point_end` because as of
/// geozero v0.12, `point_begin` and `point_end` are **not** called for each point in a
/// MultiPoint
/// <https://github.com/georust/geozero/pull/183/files#diff-a583e23825ff28368eabfdbfdc362c6512e42097024d548fb18d88409feba76aR142-R143>
pub(crate) fn process_point_as_coord<P: GeomProcessor>(
    geom: &impl PointTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use geo_traits::Dimensions;

    if let Some(coord) = geom.coord() {
        match coord.dim() {
            Dimensions::Xy | Dimensions::Unknown(2) => {
                processor.xy(coord.x(), coord.y(), coord_idx)?
            }
            Dimensions::Xyz | Dimensions::Unknown(3) => processor.coordinate(
                coord.x(),
                coord.y(),
                Some(unsafe { coord.nth_unchecked(2) }),
                None,
                None,
                None,
                coord_idx,
            )?,
            Dimensions::Xym => processor.coordinate(
                coord.x(),
                coord.y(),
                None,
                Some(unsafe { coord.nth_unchecked(2) }),
                None,
                None,
                coord_idx,
            )?,
            Dimensions::Xyzm | Dimensions::Unknown(4) => processor.coordinate(
                coord.x(),
                coord.y(),
                Some(unsafe { coord.nth_unchecked(2) }),
                Some(unsafe { coord.nth_unchecked(3) }),
                None,
                None,
                coord_idx,
            )?,
            d => {
                return Err(geozero::error::GeozeroError::Geometry(format!(
                    "Unexpected dimension {d:?}",
                )));
            }
        };
    } else {
        processor.empty_point(coord_idx)?;
    }

    Ok(())
}

impl GeozeroGeometry for Point<'_> {
    fn process_geom<P: GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
    {
        process_point(self, 0, processor)
    }
}
