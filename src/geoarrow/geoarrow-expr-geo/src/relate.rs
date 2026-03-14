use arrow_array::BooleanArray;
use geo::{Relate, relate::IntersectionMatrix};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn relate_boolean(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoArrowResult<BooleanArray> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Input arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _relate_impl, relate_cb)
    }
}

fn _relate_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanArray::builder(left_array.len());

    for (maybe_left, maybe_right) in left_array.iter().zip(right_array.iter()) {
        match (maybe_left, maybe_right) {
            (Some(left), Some(right)) => {
                let left_geom = geometry_to_geo(&left?)?;
                let right_geom = geometry_to_geo(&right?)?;
                let matrix = left_geom.relate(&right_geom);
                builder.append_value(relate_cb(matrix));
            }
            _ => {
                // If either is null, the result is null
                builder.append_null();
            }
        }
    }

    Ok(builder.finish())
}
