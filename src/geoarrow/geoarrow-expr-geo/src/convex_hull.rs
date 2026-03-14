use geo::ConvexHull;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PolygonArray, builder::PolygonBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{CoordType, Dimension, PolygonType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn convex_hull(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    downcast_geoarrow_array!(array, convex_hull_impl, coord_type)
}

fn convex_hull_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    let typ = PolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let poly = geo_geom.convex_hull();
            builder.push_polygon(Some(&poly))?;
        } else {
            builder.push_polygon(None::<geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}
