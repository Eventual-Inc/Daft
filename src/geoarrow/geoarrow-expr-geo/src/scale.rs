use std::sync::Arc;

use geo::Scale;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, builder::GeometryBuilder, downcast_geoarrow_array,
};
use geoarrow_schema::{GeometryType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

/// Scale each geometry by the given x and y factors, around each geometry's centroid.
pub fn scale(
    array: &dyn GeoArrowArray,
    x_factor: f64,
    y_factor: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _scale_impl, x_factor, y_factor)
}

/// Scale each geometry by the given x and y factors, around the given origin point.
pub fn scale_around_point(
    array: &dyn GeoArrowArray,
    x_factor: f64,
    y_factor: f64,
    origin: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _scale_around_point_impl, x_factor, y_factor, origin)
}

fn _scale_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_factor: f64,
    y_factor: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let scaled = geo_geom.scale_xy(x_factor, y_factor);
            builder.push_geometry(Some(&scaled))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _scale_around_point_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_factor: f64,
    y_factor: f64,
    origin: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let scaled = geo_geom.scale_around_point(x_factor, y_factor, origin);
            builder.push_geometry(Some(&scaled))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}
