use std::sync::Arc;

use geo::Rotate;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, builder::GeometryBuilder, downcast_geoarrow_array,
};
use geoarrow_schema::{GeometryType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

/// Rotate each geometry around its centroid by the given angle in degrees.
pub fn rotate_around_centroid(
    array: &dyn GeoArrowArray,
    degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _rotate_centroid_impl, degrees)
}

/// Rotate each geometry around its bounding box center by the given angle in degrees.
pub fn rotate_around_center(
    array: &dyn GeoArrowArray,
    degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _rotate_center_impl, degrees)
}

/// Rotate each geometry around a given point by the given angle in degrees.
pub fn rotate_around_point(
    array: &dyn GeoArrowArray,
    degrees: f64,
    point: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _rotate_point_impl, degrees, point)
}

fn _rotate_centroid_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let rotated = geo_geom.rotate_around_centroid(degrees);
            builder.push_geometry(Some(&rotated))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rotate_center_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let rotated = geo_geom.rotate_around_center(degrees);
            builder.push_geometry(Some(&rotated))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rotate_point_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    degrees: f64,
    point: geo::Point,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let rotated = geo_geom.rotate_around_point(degrees, point);
            builder.push_geometry(Some(&rotated))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}
