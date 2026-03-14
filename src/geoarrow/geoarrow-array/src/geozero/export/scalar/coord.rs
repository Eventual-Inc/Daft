use geo_traits::CoordTrait;
use geozero::GeomProcessor;

pub(crate) fn process_coord<P: GeomProcessor>(
    coord: &impl CoordTrait<T = f64>,
    coord_idx: usize,
    processor: &mut P,
) -> geozero::error::Result<()> {
    use geo_traits::Dimensions;

    match coord.dim() {
        Dimensions::Xy | Dimensions::Unknown(2) => processor.xy(coord.x(), coord.y(), coord_idx)?,
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
    Ok(())
}
