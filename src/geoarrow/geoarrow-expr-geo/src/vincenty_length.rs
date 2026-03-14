use arrow_array::Float64Array;
use geo::VincentyLength;
use geo_traits::{
    GeometryTrait,
    to_geo::{ToGeoLine, ToGeoLineString, ToGeoMultiLineString},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

pub fn vincenty_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _vincenty_length_impl)
}

fn _vincenty_length_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for geom in array.iter() {
        if let Some(geom) = geom {
            match geom?.as_type() {
                geo_traits::GeometryType::Line(l) => {
                    let length = l.to_line().vincenty_length().unwrap_or(0.0);
                    result.append_value(length);
                }
                geo_traits::GeometryType::LineString(ls) => {
                    let length = ls.to_line_string().vincenty_length().unwrap_or(0.0);
                    result.append_value(length);
                }
                geo_traits::GeometryType::MultiLineString(mls) => {
                    let length = mls.to_multi_line_string().vincenty_length().unwrap_or(0.0);
                    result.append_value(length);
                }
                _ => result.append_value(0.0),
            }
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}
