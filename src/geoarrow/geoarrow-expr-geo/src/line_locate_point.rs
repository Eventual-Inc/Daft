use arrow_array::Float64Array;
use geo::LineLocatePoint;
use geo_traits::{GeometryTrait, to_geo::ToGeoLineString};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn line_locate_point(
    line_array: &dyn GeoArrowArray,
    point_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    if line_array.len() != point_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(line_array, point_array, _line_locate_point_impl)
    }
}

fn _line_locate_point_impl<'a>(
    line_array: &'a impl GeoArrowArrayAccessor<'a>,
    point_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Array::builder(line_array.len());

    for (maybe_line, maybe_point) in line_array.iter().zip(point_array.iter()) {
        match (maybe_line, maybe_point) {
            (Some(line), Some(point)) => {
                let line_geom = line?;
                let point_geom = geometry_to_geo(&point?)?;

                match line_geom.as_type() {
                    geo_traits::GeometryType::LineString(ls) => {
                        let geo_ls = ls.to_line_string();
                        if let geo::Geometry::Point(geo_pt) = point_geom {
                            match geo_ls.line_locate_point(&geo_pt) {
                                Some(fraction) => builder.append_value(fraction),
                                None => builder.append_null(),
                            }
                        } else {
                            builder.append_null();
                        }
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
