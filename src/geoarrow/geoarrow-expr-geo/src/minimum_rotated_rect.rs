use geo::MinimumRotatedRect;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PolygonArray, builder::PolygonBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{CoordType, Dimension, PolygonType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn minimum_rotated_rect(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    downcast_geoarrow_array!(array, minimum_rotated_rect_impl, coord_type)
}

fn minimum_rotated_rect_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    let typ = PolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let poly = geo_geom.minimum_rotated_rect();
            builder.push_polygon(poly.as_ref())?;
        } else {
            builder.push_polygon(None::<geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}
