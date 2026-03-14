use geo::BoundingRect;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PointArray, builder::PointBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{CoordType, Dimension, PointType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn center(array: &dyn GeoArrowArray, coord_type: CoordType) -> GeoArrowResult<PointArray> {
    downcast_geoarrow_array!(array, _center_impl, coord_type)
}

fn _center_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    let typ = PointType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PointBuilder::with_capacity(typ, array.len());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let center_point = geo_geom
                .bounding_rect()
                .map(|rect| geo::Point::from(rect.center()));
            builder.push_point(center_point.as_ref());
        } else {
            builder.push_null();
        }
    }

    Ok(builder.finish())
}
