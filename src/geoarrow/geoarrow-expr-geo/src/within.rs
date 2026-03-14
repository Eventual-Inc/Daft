use arrow_array::BooleanArray;
use geo::Within;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn within(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<BooleanArray> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _within_impl)
    }
}

fn _within_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanArray::builder(left_array.len());

    for (maybe_left, maybe_right) in left_array.iter().zip(right_array.iter()) {
        match (maybe_left, maybe_right) {
            (Some(left), Some(right)) => {
                let left_geom = geometry_to_geo(&left?)?;
                let right_geom = geometry_to_geo(&right?)?;
                let result = left_geom.is_within(&right_geom);
                builder.append_value(result);
            }
            _ => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}
