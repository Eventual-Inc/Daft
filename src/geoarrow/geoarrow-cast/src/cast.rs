//! Cast kernels to convert [`GeoArrowArray`] to other geometry types.

use std::sync::Arc;

use arrow_schema::ArrowError;
use geoarrow_array::{
    GeoArrowArray, GeoArrowArrayAccessor,
    array::{GeometryArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray},
    builder::{
        GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
        MultiPolygonBuilder, PointBuilder, PolygonBuilder,
    },
    capacity::{LineStringCapacity, PolygonCapacity},
    cast::{AsGeoArrowArray, from_wkb, from_wkt, to_wkb, to_wkb_view, to_wkt, to_wkt_view},
};
use geoarrow_schema::{GeoArrowType, error::GeoArrowResult};

/// Cast a [`GeoArrowArray`] to another [`GeoArrowType`].
///
/// ### Criteria:
///
/// - Dimension must be compatible:
///     - If the source array and destination type are both dimension-aware, then their dimensions
///       must match.
///     - Casts from dimension-aware to dimensionless arrays (`GeometryArray`, `WkbArray`,
///       `WkbViewArray`, `WktArray`, `WktViewArray`) are always allowed.
/// - GeoArrow [`Metadata`][geoarrow_schema::Metadata] on the [`GeoArrowType`] must match. Use
///   [`GeoArrowArray::with_metadata`]
///   to change the metadata on an array.
///
/// ### Infallible casts:
///
/// As long as the above criteria are met, these casts will always succeed without erroring.
///
/// - The same geometry type with different coord types.
/// - Any source array type to `Geometry`, `Wkb`, `LargeWkb`, `WkbView`, `Wkt`, `LargeWkt`, or
///   `WktView`.
/// - `Point` to `MultiPoint`
/// - `LineString` to `MultiLineString`
/// - `Polygon` to `MultiPolygon`
///
/// ### Fallible casts:
///
/// - `Geometry` to any other native type.
/// - Parsing `WKB` or `WKT` to any native type other than `Geometry`.
/// - `MultiPoint` to `Point`
/// - `MultiLineString` to `LineString`
/// - `MultiPolygon` to `Polygon`
///
// TODO: need to check this behavior:
//
//     - Casts from dimensionless arrays to dimension-aware arrays are never allowed.
#[allow(clippy::collapsible_if)]
pub fn cast(
    array: &dyn GeoArrowArray,
    to_type: &GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // We want to error if the dimensions aren't compatible, but allow conversions to
    // `GeometryArray`, `WKB`, etc where the target array isn't parameterized by a specific
    // dimension.
    if let (Some(from_dim), Some(to_dim)) = (array.data_type().dimension(), to_type.dimension()) {
        if from_dim != to_dim {
            return Err(ArrowError::CastError(format!(
                "Cannot cast from {from_dim:?} to {to_dim:?}: incompatible dimensions",
            ))
            .into());
        }
    }

    if array.data_type().metadata() != to_type.metadata() {
        return Err(ArrowError::CastError(format!(
            "Cannot cast from {:?} to {:?}: incompatible metadata",
            array.data_type().metadata(),
            to_type.metadata(),
        ))
        .into());
    }

    use GeoArrowType::*;
    let out: Arc<dyn GeoArrowArray> = match (array.data_type(), to_type) {
        (Point(_), Point(to_type)) => {
            let array = array.as_point();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (Point(_), MultiPoint(to_type)) => {
            let mp_array = MultiPointArray::from(array.as_point().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (Point(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_point().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (LineString(_), LineString(to_type)) => {
            let array = array.as_line_string();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (LineString(_), MultiLineString(to_type)) => {
            let mp_array = MultiLineStringArray::from(array.as_line_string().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (LineString(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_line_string().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), Polygon(to_type)) => {
            let array = array.as_polygon();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), MultiPolygon(to_type)) => {
            let mp_array = MultiPolygonArray::from(array.as_polygon().clone());
            Arc::new(mp_array.into_coord_type(to_type.coord_type()))
        }
        (Polygon(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_polygon().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiPoint(_), Point(to_type)) => {
            let mut builder = PointBuilder::with_capacity(to_type.clone(), array.len());
            for geom in array.as_multi_point().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiPoint(_), MultiPoint(to_type)) => {
            let array = array.as_multi_point();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiPoint(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_point().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiLineString(_), LineString(to_type)) => {
            let ml_array = array.as_multi_line_string();
            let ml_capacity = ml_array.buffer_lengths();
            let ls_capacity =
                LineStringCapacity::new(ml_capacity.coord_capacity(), ml_capacity.geom_capacity());
            let mut builder = LineStringBuilder::with_capacity(to_type.clone(), ls_capacity);
            for geom in array.as_multi_line_string().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiLineString(_), MultiLineString(to_type)) => {
            let array = array.as_multi_line_string();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiLineString(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_line_string().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (MultiPolygon(_), Polygon(to_type)) => {
            let mp_array = array.as_multi_polygon();
            let mp_capacity = mp_array.buffer_lengths();
            let p_capacity = PolygonCapacity::new(
                mp_capacity.coord_capacity(),
                mp_capacity.ring_capacity(),
                mp_capacity.geom_capacity(),
            );
            let mut builder = PolygonBuilder::with_capacity(to_type.clone(), p_capacity);
            for geom in mp_array.iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (MultiPolygon(_), MultiPolygon(to_type)) => {
            let array = array.as_multi_polygon();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (MultiPolygon(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_multi_polygon().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (Geometry(_), Point(to_type)) => {
            let mut builder = PointBuilder::with_capacity(to_type.clone(), array.len());
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), LineString(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let ls_capacity = g_capacity.line_string(to_type.dimension());
            let mut builder = LineStringBuilder::with_capacity(to_type.clone(), ls_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), Polygon(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let p_capacity = g_capacity.polygon(to_type.dimension());
            let mut builder = PolygonBuilder::with_capacity(to_type.clone(), p_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), MultiPoint(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let mp_capacity = g_capacity.multi_point(to_type.dimension());
            let mut builder = MultiPointBuilder::with_capacity(to_type.clone(), mp_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), MultiLineString(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let ml_capacity = g_capacity.multi_line_string(to_type.dimension());
            let mut builder = MultiLineStringBuilder::with_capacity(to_type.clone(), ml_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), MultiPolygon(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let mp_capacity = g_capacity.multi_polygon(to_type.dimension());
            let mut builder = MultiPolygonBuilder::with_capacity(to_type.clone(), mp_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), GeometryCollection(to_type)) => {
            let g_array = array.as_geometry();
            let g_capacity = g_array.buffer_lengths();
            let gc_capacity = g_capacity.geometry_collection(to_type.dimension());
            let mut builder =
                GeometryCollectionBuilder::with_capacity(to_type.clone(), gc_capacity);
            for geom in array.as_geometry().iter() {
                builder.push_geometry(geom.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish())
        }
        (Geometry(_), Geometry(to_type)) => {
            let array = array.as_geometry();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (GeometryCollection(_), GeometryCollection(to_type)) => {
            let array = array.as_geometry_collection();
            Arc::new(array.clone().into_coord_type(to_type.coord_type()))
        }
        (GeometryCollection(_), Geometry(to_type)) => {
            let geom_array = GeometryArray::from(array.as_geometry_collection().clone());
            Arc::new(geom_array.into_coord_type(to_type.coord_type()))
        }
        (_, Wkb(_)) => Arc::new(to_wkb::<i32>(array)?),
        (_, LargeWkb(_)) => Arc::new(to_wkb::<i64>(array)?),
        (_, WkbView(_)) => Arc::new(to_wkb_view(array)?),
        (_, Wkt(_)) => Arc::new(to_wkt::<i32>(array)?),
        (_, LargeWkt(_)) => Arc::new(to_wkt::<i64>(array)?),
        (_, WktView(_)) => Arc::new(to_wkt_view(array)?),
        (Wkb(_), _) => from_wkb(array.as_wkb::<i32>(), to_type.clone())?,
        (LargeWkb(_), _) => from_wkb(array.as_wkb::<i64>(), to_type.clone())?,
        (WkbView(_), _) => from_wkb(array.as_wkb_view(), to_type.clone())?,
        (Wkt(_), _) => from_wkt(array.as_wkt::<i32>(), to_type.clone())?,
        (LargeWkt(_), _) => from_wkt(array.as_wkt::<i64>(), to_type.clone())?,
        (WktView(_), _) => from_wkt(array.as_wkt_view(), to_type.clone())?,
        (_, _) => {
            return Err(ArrowError::CastError(format!(
                "Unsupported cast from {:?} to {:?}",
                array.data_type(),
                to_type
            ))
            .into());
        }
    };
    Ok(out)
}

// #[cfg(test)]
// mod test {
//     use geoarrow_array::{IntoArrow, builder::MultiPointBuilder, test};
//     use geoarrow_schema::{
//         CoordType, Dimension, GeometryType, LineStringType, MultiLineStringType, MultiPointType,
//         MultiPolygonType, PointType, PolygonType, WkbType,
//     };
//     use wkt::wkt;

//     use super::*;

//     #[test]
//     fn test_point() {
//         let array = test::point::array(CoordType::Interleaved, Dimension::XY);

//         // Cast to the same type
//         let array2 = cast(&array, &array.data_type()).unwrap();
//         assert_eq!(&array, array2.as_point());

//         // Cast to other coord type
//         let p_type = PointType::new(Dimension::XY, array.data_type().metadata().clone())
//             .with_coord_type(CoordType::Separated);
//         let array3 = cast(&array, &p_type.into()).unwrap();
//         assert_eq!(
//             array3.as_point().extension_type().coord_type(),
//             CoordType::Separated
//         );

//         // Cast to multi point
//         let mp_type = MultiPointType::new(Dimension::XY, array.data_type().metadata().clone())
//             .with_coord_type(CoordType::Interleaved);
//         let mp_array = cast(&array, &mp_type.into()).unwrap();
//         assert!(mp_array.as_multi_point_opt().is_some());

//         // Cast to geometry
//         let mp_type = GeometryType::new(array.data_type().metadata().clone())
//             .with_coord_type(CoordType::Interleaved);
//         let mp_array = cast(&array, &mp_type.into()).unwrap();
//         assert!(mp_array.as_geometry_opt().is_some());
//     }

//     #[test]
//     fn cast_to_wkb() {
//         let array = test::point::array(CoordType::Interleaved, Dimension::XY);

//         let wkb_type = GeoArrowType::Wkb(WkbType::new(array.data_type().metadata().clone()));
//         let wkb_array = cast(&array, &wkb_type).unwrap();
//         assert!(wkb_array.as_wkb_opt::<i32>().is_some());

//         let large_wkb_type =
//             GeoArrowType::LargeWkb(WkbType::new(array.data_type().metadata().clone()));
//         let wkb_array = cast(&array, &large_wkb_type).unwrap();
//         assert!(wkb_array.as_wkb_opt::<i64>().is_some());
//     }

//     #[test]
//     fn downcast_multi_points_to_points() {
//         let mp1 = wkt! { MULTIPOINT(0.0 0.0) };
//         let mp2 = wkt! { MULTIPOINT(1.0 2.0) };
//         let mp3 = wkt! { MULTIPOINT(3.0 4.0) };

//         let typ = MultiPointType::new(Dimension::XY, Default::default());
//         let mp_arr = MultiPointBuilder::from_multi_points(&[mp1, mp2, mp3], typ).finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = PointType::new(dim, metadata).with_coord_type(coord_type);
//         let p_arr = cast(&mp_arr, &p_type.into()).unwrap();
//         assert!(p_arr.as_point_opt().is_some());
//     }

//     #[test]
//     fn downcast_multi_points_to_points_fails() {
//         let mp1 = wkt! { MULTIPOINT(0.0 0.0) };
//         let mp2 = wkt! { MULTIPOINT(1.0 2.0) };
//         let mp3 = wkt! { MULTIPOINT(3.0 4.0, 5.0 6.0) };

//         let typ = MultiPointType::new(Dimension::XY, Default::default());
//         let mp_arr = MultiPointBuilder::from_multi_points(&[mp1, mp2, mp3], typ).finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = PointType::new(dim, metadata).with_coord_type(coord_type);
//         assert!(cast(&mp_arr, &p_type.into()).is_err());
//     }

//     #[test]
//     fn downcast_multi_line_strings_to_line_strings() {
//         let geoms = geoarrow_test::raw::multilinestring::xy::geoms();
//         let single = geoms[0].clone().unwrap();

//         let typ = MultiLineStringType::new(Dimension::XY, Default::default());
//         let mp_arr = MultiLineStringBuilder::from_multi_line_strings(
//             &[single.clone(), single.clone(), single],
//             typ,
//         )
//         .finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = LineStringType::new(dim, metadata).with_coord_type(coord_type);
//         let p_arr = cast(&mp_arr, &p_type.into()).unwrap();
//         assert!(p_arr.as_line_string_opt().is_some());
//     }

//     #[test]
//     fn downcast_multi_line_strings_to_line_strings_fails() {
//         let geoms = geoarrow_test::raw::multilinestring::xy::geoms();
//         let single = geoms[0].clone().unwrap();
//         let multi = geoms[1].clone().unwrap();

//         let typ = MultiLineStringType::new(Dimension::XY, Default::default());
//         let mp_arr =
//             MultiLineStringBuilder::from_multi_line_strings(&[single.clone(), single, multi], typ)
//                 .finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = LineStringType::new(dim, metadata).with_coord_type(coord_type);
//         assert!(cast(&mp_arr, &p_type.into()).is_err());
//     }

//     #[test]
//     fn downcast_multi_polygons_to_polygons() {
//         let geoms = geoarrow_test::raw::multipolygon::xy::geoms();
//         let single = geoms[0].clone().unwrap();

//         let typ = MultiPolygonType::new(Dimension::XY, Default::default());
//         let mp_arr = MultiPolygonBuilder::from_multi_polygons(
//             &[single.clone(), single.clone(), single],
//             typ,
//         )
//         .finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = PolygonType::new(dim, metadata).with_coord_type(coord_type);
//         let p_arr = cast(&mp_arr, &p_type.into()).unwrap();
//         assert!(p_arr.as_polygon_opt().is_some());
//     }

//     #[test]
//     fn downcast_multi_polygons_to_polygons_fails() {
//         let geoms = geoarrow_test::raw::multipolygon::xy::geoms();
//         let single = geoms[0].clone().unwrap();
//         let multi = geoms[1].clone().unwrap();

//         let typ = MultiPolygonType::new(Dimension::XY, Default::default());
//         let mp_arr =
//             MultiPolygonBuilder::from_multi_polygons(&[single.clone(), single, multi], typ)
//                 .finish();
//         let (coord_type, dim, metadata) = mp_arr.extension_type().clone().into_inner();
//         let p_type = PolygonType::new(dim, metadata).with_coord_type(coord_type);
//         assert!(cast(&mp_arr, &p_type.into()).is_err());
//     }

//     #[test]
//     fn downcast_geometry_to_point() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::point::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_point(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_line_string() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::linestring::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_line_string(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_polygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::polygon::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_polygon(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_multi_point() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::multipoint::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_multi_point(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_multi_line_string() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::multilinestring::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_multi_line_string(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_multi_polygon() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 let array = test::multipolygon::array(coord_type, dim);
//                 let orig_type = array.data_type().clone();
//                 let g_array = GeometryArray::from(array.clone());

//                 let casted = cast(&g_array, &orig_type).unwrap();
//                 assert_eq!(casted.as_multi_polygon(), &array);
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_geometry_collection() {
//         for coord_type in [CoordType::Interleaved, CoordType::Separated] {
//             for dim in [
//                 Dimension::XY,
//                 Dimension::XYZ,
//                 Dimension::XYM,
//                 Dimension::XYZM,
//             ] {
//                 for prefer_multi in [false, true] {
//                     let array = test::geometrycollection::array(coord_type, dim, prefer_multi);
//                     let orig_type = array.data_type().clone();
//                     let g_array = GeometryArray::from(array.clone());

//                     let casted = cast(&g_array, &orig_type).unwrap();
//                     assert_eq!(casted.as_geometry_collection(), &array);
//                 }
//             }
//         }
//     }

//     #[test]
//     fn downcast_geometry_to_point_fails() {
//         let array = test::geometry::array(Default::default(), false);
//         let point_type = PointType::new(Dimension::XY, Default::default());
//         assert!(cast(&array, &point_type.into()).is_err());
//     }
// }
