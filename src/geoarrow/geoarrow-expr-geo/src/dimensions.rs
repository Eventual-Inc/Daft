use arrow_array::{BooleanArray, builder::BooleanBuilder};
use geo::HasDimensions;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::to_geo::geometry_to_geo;

pub fn is_empty(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_empty_impl)
}

fn _is_empty_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            builder.append_value(geo_geom.is_empty());
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}
