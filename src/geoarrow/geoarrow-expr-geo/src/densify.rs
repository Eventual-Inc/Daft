use std::sync::Arc;

use geo::{Densify, Euclidean};
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

pub fn densify(
    array: &dyn GeoArrowArray,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) | GeometryCollection(_) | Rect(_) => {
            Ok(copy_geoarrow_array_ref(array))
        }
        LineString(_) => _densify_linestring(array.as_line_string(), max_distance),
        Polygon(_) => _densify_polygon(array.as_polygon(), max_distance),
        MultiLineString(_) => _densify_multi_linestring(array.as_multi_line_string(), max_distance),
        MultiPolygon(_) => _densify_multi_polygon(array.as_multi_polygon(), max_distance),
        _ => downcast_geoarrow_array!(array, _densify_geometry_impl, max_distance),
    }
}

fn _densify_linestring(
    array: &LineStringArray,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = LineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_line_string();
            builder.push_line_string(Some(&Euclidean.densify(&geo_geom, max_distance)))?;
        } else {
            builder.push_line_string(None::<&geo::LineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _densify_polygon(
    array: &PolygonArray,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = PolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_polygon();
            builder.push_polygon(Some(&Euclidean.densify(&geo_geom, max_distance)))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _densify_multi_linestring(
    array: &MultiLineStringArray,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiLineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_line_string();
            builder.push_multi_line_string(Some(&Euclidean.densify(&geo_geom, max_distance)))?;
        } else {
            builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _densify_multi_polygon(
    array: &MultiPolygonArray,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiPolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_polygon();
            builder.push_multi_polygon(Some(&Euclidean.densify(&geo_geom, max_distance)))?;
        } else {
            builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _densify_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    max_distance: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let densified = _densify_geometry(&geo_geom, max_distance);
            builder.push_geometry(Some(&densified))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _densify_geometry(geom: &geo::Geometry, max_distance: f64) -> geo::Geometry {
    match geom {
        geo::Geometry::LineString(g) => {
            geo::Geometry::LineString(Euclidean.densify(g, max_distance))
        }
        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(Euclidean.densify(g, max_distance)),
        geo::Geometry::MultiLineString(g) => {
            geo::Geometry::MultiLineString(Euclidean.densify(g, max_distance))
        }
        geo::Geometry::MultiPolygon(g) => {
            geo::Geometry::MultiPolygon(Euclidean.densify(g, max_distance))
        }
        _ => geom.clone(),
    }
}
