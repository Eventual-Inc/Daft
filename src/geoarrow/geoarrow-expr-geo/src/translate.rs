use std::sync::Arc;

use geo::Translate;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, builder::GeometryBuilder, downcast_geoarrow_array,
};
use geoarrow_schema::{GeometryType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

/// Translate each geometry by the given x and y offsets.
pub fn translate(
    array: &dyn GeoArrowArray,
    x_offset: f64,
    y_offset: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _translate_impl, x_offset, y_offset)
}

fn _translate_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_offset: f64,
    y_offset: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let translated = geo_geom.translate(x_offset, y_offset);
            builder.push_geometry(Some(&translated))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}
