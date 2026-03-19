use geo::{Euclidean, InterpolateLine};
use geo_traits::{GeometryTrait, to_geo::ToGeoLineString};
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PointArray, builder::PointBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{CoordType, Dimension, PointType, error::GeoArrowResult};

pub fn line_interpolate_point(
    array: &dyn GeoArrowArray,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    downcast_geoarrow_array!(array, _line_interpolate_point_impl, fraction, coord_type)
}

fn _line_interpolate_point_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    let typ = PointType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PointBuilder::with_capacity(typ, array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            match geom?.as_type() {
                geo_traits::GeometryType::LineString(ls) => {
                    let geo_ls = ls.to_line_string();
                    let point = Euclidean.point_at_ratio_from_start(&geo_ls, fraction);
                    builder.push_point(point.as_ref());
                }
                _ => {
                    builder.push_null();
                }
            }
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish())
}
