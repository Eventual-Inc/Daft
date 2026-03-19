use std::sync::Arc;

use geo::Skew;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, builder::GeometryBuilder, downcast_geoarrow_array,
};
use geoarrow_schema::{GeometryType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

/// Skew each geometry by the given x and y degrees, around each geometry's centroid.
pub fn skew(
    array: &dyn GeoArrowArray,
    x_degrees: f64,
    y_degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _skew_impl, x_degrees, y_degrees)
}

/// Skew each geometry by the given x and y degrees, around the given origin point.
pub fn skew_around_point(
    array: &dyn GeoArrowArray,
    x_degrees: f64,
    y_degrees: f64,
    origin: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _skew_around_point_impl, x_degrees, y_degrees, origin)
}

fn _skew_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_degrees: f64,
    y_degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let skewed = geo_geom.skew_xy(x_degrees, y_degrees);
            builder.push_geometry(Some(&skewed))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _skew_around_point_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_degrees: f64,
    y_degrees: f64,
    origin: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let skewed = geo_geom.skew_around_point(x_degrees, y_degrees, origin);
            builder.push_geometry(Some(&skewed))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}
