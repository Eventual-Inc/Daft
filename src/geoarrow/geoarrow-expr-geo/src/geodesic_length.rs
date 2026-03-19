use arrow_array::Float64Array;
use geo::{Geodesic, Length};
use geo_traits::{
    GeometryTrait,
    to_geo::{ToGeoLine, ToGeoLineString, ToGeoMultiLineString},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

pub fn geodesic_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _geodesic_length_impl)
}

fn _geodesic_length_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for geom in array.iter() {
        if let Some(geom) = geom {
            match geom?.as_type() {
                geo_traits::GeometryType::Line(l) => {
                    result.append_value(Geodesic.length(&l.to_line()))
                }
                geo_traits::GeometryType::LineString(ls) => {
                    result.append_value(Geodesic.length(&ls.to_line_string()))
                }
                geo_traits::GeometryType::MultiLineString(mls) => {
                    result.append_value(Geodesic.length(&mls.to_multi_line_string()))
                }
                _ => result.append_value(0.0),
            }
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}
