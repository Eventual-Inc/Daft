use arrow_array::Float64Array;
use geo::{Distance, Euclidean};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn euclidean_distance(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _distance_impl)
    }
}

fn _distance_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Array::builder(left_array.len());

    for (left, right) in left_array.iter().zip(right_array.iter()) {
        match (left, right) {
            (Some(left), Some(right)) => {
                let left_geom = geometry_to_geo(&left?)?;
                let right_geom = geometry_to_geo(&right?)?;
                let result = Euclidean.distance(&left_geom, &right_geom);
                builder.append_value(result);
            }
            (_, _) => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}
