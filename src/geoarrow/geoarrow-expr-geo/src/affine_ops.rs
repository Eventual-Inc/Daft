use std::sync::Arc;

use geo::AffineOps;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, builder::GeometryBuilder, downcast_geoarrow_array,
};
use geoarrow_schema::{GeometryType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn affine_transform(
    array: &dyn GeoArrowArray,
    transform: &geo::AffineTransform,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _affine_transform_impl, transform)
}

fn _affine_transform_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    transform: &geo::AffineTransform,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let mut geo_geom = geometry_to_geo(&geom?)?;
            geo_geom.affine_transform_mut(transform);
            builder.push_geometry(Some(&geo_geom))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}
