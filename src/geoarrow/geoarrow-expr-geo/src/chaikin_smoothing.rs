use std::sync::Arc;

use geo::ChaikinSmoothing;
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

pub fn chaikin_smoothing(
    array: &dyn GeoArrowArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) | GeometryCollection(_) | Rect(_) => {
            Ok(copy_geoarrow_array_ref(array))
        }
        LineString(_) => _chaikin_linestring(array.as_line_string(), n_iterations),
        Polygon(_) => _chaikin_polygon(array.as_polygon(), n_iterations),
        MultiLineString(_) => _chaikin_multi_linestring(array.as_multi_line_string(), n_iterations),
        MultiPolygon(_) => _chaikin_multi_polygon(array.as_multi_polygon(), n_iterations),
        _ => downcast_geoarrow_array!(array, _chaikin_geometry_impl, n_iterations),
    }
}

fn _chaikin_linestring(
    array: &LineStringArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = LineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_line_string();
            builder.push_line_string(Some(&geo_geom.chaikin_smoothing(n_iterations)))?;
        } else {
            builder.push_line_string(None::<&geo::LineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _chaikin_polygon(
    array: &PolygonArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = PolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_polygon();
            builder.push_polygon(Some(&geo_geom.chaikin_smoothing(n_iterations)))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _chaikin_multi_linestring(
    array: &MultiLineStringArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiLineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_line_string();
            builder.push_multi_line_string(Some(&geo_geom.chaikin_smoothing(n_iterations)))?;
        } else {
            builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _chaikin_multi_polygon(
    array: &MultiPolygonArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiPolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_polygon();
            builder.push_multi_polygon(Some(&geo_geom.chaikin_smoothing(n_iterations)))?;
        } else {
            builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _chaikin_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let smoothed = _chaikin_geometry(&geo_geom, n_iterations);
            builder.push_geometry(Some(&smoothed))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _chaikin_geometry(geom: &geo::Geometry, n_iterations: usize) -> geo::Geometry {
    match geom {
        geo::Geometry::LineString(g) => {
            geo::Geometry::LineString(g.chaikin_smoothing(n_iterations))
        }
        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(g.chaikin_smoothing(n_iterations)),
        geo::Geometry::MultiLineString(g) => {
            geo::Geometry::MultiLineString(g.chaikin_smoothing(n_iterations))
        }
        geo::Geometry::MultiPolygon(g) => {
            geo::Geometry::MultiPolygon(g.chaikin_smoothing(n_iterations))
        }
        _ => geom.clone(),
    }
}
