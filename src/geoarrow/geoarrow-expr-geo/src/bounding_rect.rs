use geo::BoundingRect;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::RectArray, builder::RectBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{BoxType, Dimension, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn bounding_rect(array: &dyn GeoArrowArray) -> GeoArrowResult<RectArray> {
    downcast_geoarrow_array!(array, _bounding_rect_impl)
}

fn _bounding_rect_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<RectArray> {
    let typ = BoxType::new(Dimension::XY, array.data_type().metadata().clone());
    let mut builder = RectBuilder::with_capacity(typ, array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let rect = geo_geom.bounding_rect();
            builder.push_rect(rect.as_ref());
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish())
}
