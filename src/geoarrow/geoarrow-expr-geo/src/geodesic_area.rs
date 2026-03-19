use arrow_array::{Float64Array, builder::Float64Builder};
use arrow_buffer::NullBuffer;
use geo::GeodesicArea;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{GeoArrowType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn geodesic_area_signed(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _geodesic_area_signed_impl)
}

pub fn geodesic_area_unsigned(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _geodesic_area_unsigned_impl)
}

pub fn geodesic_perimeter(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _geodesic_perimeter_impl)
}

fn _zeros(len: usize, nulls: Option<NullBuffer>) -> Float64Array {
    let values = vec![0.0f64; len];
    Float64Array::new(values.into(), nulls)
}

fn _geodesic_area_signed_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(_zeros(array.len(), array.logical_nulls()))
        }
        _ => _geodesic_area_impl(array, GeodesicArea::geodesic_area_signed),
    }
}

fn _geodesic_area_unsigned_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) => {
            Ok(_zeros(array.len(), array.logical_nulls()))
        }
        _ => _geodesic_area_impl(array, GeodesicArea::geodesic_area_unsigned),
    }
}

fn _geodesic_perimeter_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) => Ok(_zeros(array.len(), array.logical_nulls())),
        _ => _geodesic_area_impl(array, GeodesicArea::geodesic_perimeter),
    }
}

fn _geodesic_area_impl<'a, F: Fn(&geo::Geometry) -> f64>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    area_fn: F,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(area_fn(&geo_geom));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
