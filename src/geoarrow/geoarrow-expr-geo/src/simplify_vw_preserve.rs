use std::sync::Arc;

use geo::SimplifyVwPreserve;
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor, IntoArrow,
    array::{LineStringArray, MultiLineStringArray, MultiPolygonArray, PolygonArray},
    builder::{
        GeometryBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder,
        PolygonBuilder,
    },
    cast::AsGeoArrowArray,
    downcast_geoarrow_array,
};
use geoarrow_schema::{GeoArrowType, GeometryType, error::GeoArrowResult};

use crate::util::{copy_geoarrow_array_ref, to_geo::geometry_to_geo};

pub fn simplify_vw_preserve(
    array: &dyn GeoArrowArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) | GeometryCollection(_) | Rect(_) => {
            Ok(copy_geoarrow_array_ref(array))
        }
        LineString(_) => simplify_vw_preserve_linestring(array.as_line_string(), epsilon),
        Polygon(_) => simplify_vw_preserve_polygon(array.as_polygon(), epsilon),
        MultiLineString(_) => {
            simplify_vw_preserve_multi_linestring(array.as_multi_line_string(), epsilon)
        }
        MultiPolygon(_) => simplify_vw_preserve_multi_polygon(array.as_multi_polygon(), epsilon),
        _ => downcast_geoarrow_array!(array, simplify_vw_preserve_geometry_impl, epsilon),
    }
}

fn simplify_vw_preserve_linestring(
    array: &LineStringArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = LineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_line_string();
            builder.push_line_string(Some(&geo_geom.simplify_vw_preserve(epsilon)))?;
        } else {
            builder.push_line_string(None::<&geo::LineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn simplify_vw_preserve_polygon(
    array: &PolygonArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = PolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_polygon();
            builder.push_polygon(Some(&geo_geom.simplify_vw_preserve(epsilon)))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn simplify_vw_preserve_multi_linestring(
    array: &MultiLineStringArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiLineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_line_string();
            builder.push_multi_line_string(Some(&geo_geom.simplify_vw_preserve(epsilon)))?;
        } else {
            builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn simplify_vw_preserve_multi_polygon(
    array: &MultiPolygonArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiPolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_polygon();
            builder.push_multi_polygon(Some(&geo_geom.simplify_vw_preserve(epsilon)))?;
        } else {
            builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn simplify_vw_preserve_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let simplified_geom = simplify_vw_preserve_geometry(&geo_geom, &epsilon);
            builder.push_geometry(Some(&simplified_geom))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn simplify_vw_preserve_geometry(geom: &geo::Geometry, epsilon: &f64) -> geo::Geometry {
    match geom {
        geo::Geometry::LineString(g) => geo::Geometry::LineString(g.simplify_vw_preserve(*epsilon)),
        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(g.simplify_vw_preserve(*epsilon)),
        geo::Geometry::MultiLineString(g) => {
            geo::Geometry::MultiLineString(g.simplify_vw_preserve(*epsilon))
        }
        geo::Geometry::MultiPolygon(g) => {
            geo::Geometry::MultiPolygon(g.simplify_vw_preserve(*epsilon))
        }
        _ => geom.clone(),
    }
}
