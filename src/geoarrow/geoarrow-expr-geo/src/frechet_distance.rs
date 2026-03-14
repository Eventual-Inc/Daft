use arrow_array::Float64Array;
use geo::{Euclidean, line_measures::FrechetDistance};
use geo_traits::{GeometryTrait, to_geo::ToGeoLineString};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::downcast::downcast_geoarrow_array_two_args;

pub fn frechet_distance(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _frechet_distance_impl)
    }
}

fn _frechet_distance_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Array::builder(left_array.len());

    for (maybe_left, maybe_right) in left_array.iter().zip(right_array.iter()) {
        match (maybe_left, maybe_right) {
            (Some(left), Some(right)) => {
                let left_geom = left?;
                let right_geom = right?;

                match (left_geom.as_type(), right_geom.as_type()) {
                    (
                        geo_traits::GeometryType::LineString(ls1),
                        geo_traits::GeometryType::LineString(ls2),
                    ) => {
                        let geo_ls1 = ls1.to_line_string();
                        let geo_ls2 = ls2.to_line_string();
                        builder.append_value(Euclidean.frechet_distance(&geo_ls1, &geo_ls2));
                    }
                    _ => {
                        builder.append_null();
                    }
                }
            }
            _ => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}
