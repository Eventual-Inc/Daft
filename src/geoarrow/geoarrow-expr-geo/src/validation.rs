use arrow_array::{
    BooleanArray, StringViewArray,
    builder::{BooleanBuilder, StringViewBuilder},
};
use geo::Validation;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::to_geo::geometry_to_geo;

pub fn is_valid(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_valid_impl)
}

fn _is_valid_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(geo_geom.is_valid());
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

pub fn is_valid_reason(array: &dyn GeoArrowArray) -> GeoArrowResult<StringViewArray> {
    downcast_geoarrow_array!(array, _is_valid_reason_impl)
}

fn _is_valid_reason_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<StringViewArray> {
    let mut builder = StringViewBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            if let Err(err) = geo_geom.check_validation() {
                builder.append_value(err.to_string());
            } else {
                builder.append_value("Valid Geometry");
            }
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
