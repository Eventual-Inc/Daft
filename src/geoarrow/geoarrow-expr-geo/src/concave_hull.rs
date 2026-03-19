use geo::{ConcaveHull, ConvexHull};
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, array::PolygonArray, builder::PolygonBuilder,
    downcast_geoarrow_array,
};
use geoarrow_schema::{CoordType, Dimension, PolygonType, error::GeoArrowResult};

use crate::util::to_geo::geometry_to_geo;

pub fn concave_hull(
    array: &dyn GeoArrowArray,
    concavity: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    downcast_geoarrow_array!(array, _concave_hull_impl, concavity, coord_type)
}

fn _concave_hull_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    concavity: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    let typ = PolygonType::new(Dimension::XY, array.data_type().metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = PolygonBuilder::new(typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let poly = match &geo_geom {
                geo::Geometry::Polygon(g) => g.concave_hull(concavity),
                geo::Geometry::MultiPolygon(g) => g.concave_hull(concavity),
                geo::Geometry::LineString(g) => g.concave_hull(concavity),
                geo::Geometry::MultiLineString(g) => g.concave_hull(concavity),
                geo::Geometry::MultiPoint(g) => g.concave_hull(concavity),
                _ => geo_geom.convex_hull(),
            };
            builder.push_polygon(Some(&poly))?;
        } else {
            builder.push_polygon(None::<geo::Polygon>.as_ref())?;
        }
    }

    Ok(builder.finish())
}
