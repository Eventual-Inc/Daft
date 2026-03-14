use std::sync::Arc;

use geo::RemoveRepeatedPoints;
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

pub fn remove_repeated_points(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;
    match array.data_type() {
        Point(_) | MultiPoint(_) | GeometryCollection(_) | Rect(_) => {
            Ok(copy_geoarrow_array_ref(array))
        }
        LineString(_) => _rrp_linestring(array.as_line_string()),
        Polygon(_) => _rrp_polygon(array.as_polygon()),
        MultiLineString(_) => _rrp_multi_linestring(array.as_multi_line_string()),
        MultiPolygon(_) => _rrp_multi_polygon(array.as_multi_polygon()),
        _ => downcast_geoarrow_array!(array, _rrp_geometry_impl),
    }
}

fn _rrp_linestring(array: &LineStringArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = LineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_line_string();
            builder.push_line_string(Some(&geo_geom.remove_repeated_points()))?;
        } else {
            builder.push_line_string(None::<&geo::LineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rrp_polygon(array: &PolygonArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = PolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_polygon();
            builder.push_polygon(Some(&geo_geom.remove_repeated_points()))?;
        } else {
            builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rrp_multi_linestring(array: &MultiLineStringArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiLineStringBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_line_string();
            builder.push_multi_line_string(Some(&geo_geom.remove_repeated_points()))?;
        } else {
            builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rrp_multi_polygon(array: &MultiPolygonArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let mut builder = MultiPolygonBuilder::new(array.extension_type().clone());

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geom?.to_multi_polygon();
            builder.push_multi_polygon(Some(&geo_geom.remove_repeated_points()))?;
        } else {
            builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rrp_geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_typ = GeometryType::new(array.data_type().metadata().clone());
    let mut builder = GeometryBuilder::new(geom_typ);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let cleaned = _rrp_geometry(&geo_geom);
            builder.push_geometry(Some(&cleaned))?;
        } else {
            builder.push_geometry(None::<&geo::Geometry>.as_ref())?;
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn _rrp_geometry(geom: &geo::Geometry) -> geo::Geometry {
    match geom {
        geo::Geometry::LineString(g) => geo::Geometry::LineString(g.remove_repeated_points()),
        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(g.remove_repeated_points()),
        geo::Geometry::MultiLineString(g) => {
            geo::Geometry::MultiLineString(g.remove_repeated_points())
        }
        geo::Geometry::MultiPolygon(g) => geo::Geometry::MultiPolygon(g.remove_repeated_points()),
        _ => geom.clone(),
    }
}
