use arrow_array::Float64Array;
use geo::{Euclidean, Length};
use geo_traits::{
    GeometryTrait,
    to_geo::{ToGeoLine, ToGeoLineString, ToGeoMultiLineString},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;

/// Compute the euclidean length of linear geometries in a GeoArrowArray.
///
/// Only LineString and MultiLineString geometries will have non-zero lengths.
/// Other geometry types (including polygons) will return a length of 0.0.
pub fn euclidean_length(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _length_impl)
}

pub fn _length_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Float64Array> {
    let mut result = Float64Array::builder(array.len());
    for geom in array.iter() {
        if let Some(geom) = geom {
            match geom?.as_type() {
                geo_traits::GeometryType::Line(l) => {
                    result.append_value(Euclidean.length(&l.to_line()))
                }
                geo_traits::GeometryType::LineString(ls) => {
                    result.append_value(Euclidean.length(&ls.to_line_string()))
                }
                geo_traits::GeometryType::MultiLineString(mls) => {
                    result.append_value(Euclidean.length(&mls.to_multi_line_string()))
                }
                _ => result.append_value(0.0),
            }
        } else {
            result.append_null();
        }
    }
    Ok(result.finish())
}

// #[cfg(test)]
// mod test {

//     use geo::{Euclidean, Length, LineString, MultiLineString, Point};
//     use geoarrow_array::{
//         array::PointArray,
//         builder::{LineStringBuilder, MultiLineStringBuilder, PointBuilder, WkbBuilder},
//     };
//     use geoarrow_schema::{CoordType, Dimension, PointType, WkbType};

//     use super::*;

//     #[test]
//     fn test_point() {
//         let point_type = PointType::new(Dimension::XY, Default::default());
//         let mut builder = PointBuilder::new(point_type);

//         builder.push_point(Some(&Point::new(0., 1.)));
//         builder.push_point(Some(&Point::new(2., 3.)));
//         builder.push_point(Some(&Point::new(4., 5.)));

//         let point_array: PointArray = builder.finish();
//         let result = euclidean_length(&point_array).unwrap();

//         assert_eq!(result.len(), 3);
//         assert_eq!(result.value(0), 0.0);
//         assert_eq!(result.value(1), 0.0);
//         assert_eq!(result.value(2), 0.0);
//     }

//     #[test]
//     fn test_linestring() {
//         let mut linestring_builder = LineStringBuilder::new(
//             geoarrow_schema::LineStringType::new(Dimension::XY, Default::default())
//                 .with_coord_type(CoordType::Separated),
//         );
//         let linestring_1 = LineString::from(vec![(0.0, 0.0), (3.0, 9.0)]);
//         let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 5.0)]);

//         let _ = linestring_builder.push_geometry(Some(&linestring_1));
//         let _ = linestring_builder.push_geometry(Some(&linestring_2));
//         let linestring_array = linestring_builder.finish();

//         let result = euclidean_length(&linestring_array).unwrap();

//         assert_eq!(result.len(), 2);
//         assert_eq!(result.value(0), Euclidean.length(&linestring_1));
//         assert_eq!(result.value(1), Euclidean.length(&linestring_2));
//     }

//     #[test]
//     fn test_multilinestring() {
//         let mut multi_linestring_builder = MultiLineStringBuilder::new(
//             geoarrow_schema::MultiLineStringType::new(Dimension::XY, Default::default())
//                 .with_coord_type(CoordType::Separated),
//         );
//         let linestring_1 = LineString::from(vec![(0.0, 9.0), (3.0, 4.0)]);
//         let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 3.0)]);
//         let multi_linestring_1 =
//             MultiLineString::new(vec![linestring_1.clone(), linestring_2.clone()]);
//         let linestring_3 = LineString::from(vec![(1.0, 5.0), (5.0, 6.0)]);
//         let multi_linestring_2 = MultiLineString::new(vec![linestring_3.clone()]);

//         let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_1));
//         let _ = multi_linestring_builder.push_geometry(Some(&multi_linestring_2));

//         let multi_linestring_array = multi_linestring_builder.finish();
//         let result = euclidean_length(&multi_linestring_array).unwrap();

//         assert_eq!(result.len(), 2);
//         assert_eq!(
//             result.value(0),
//             Euclidean.length(&linestring_1) + Euclidean.length(&linestring_2)
//         );
//         assert_eq!(result.value(1), Euclidean.length(&linestring_3));
//     }

//     #[test]
//     fn test_wkb_linestring() {
//         let mut wkb_builder: WkbBuilder<i32> =
//             geoarrow_array::builder::WkbBuilder::new(WkbType::new(Default::default()));
//         let linestring_1 = LineString::from(vec![(0.0, 0.0), (3.0, 4.0)]);
//         let linestring_2 = LineString::from(vec![(0.0, 0.0), (4.0, 5.0)]);
//         let _ = wkb_builder.push_geometry(Some(&linestring_1));
//         let _ = wkb_builder.push_geometry(Some(&linestring_2));
//         let wkb_array = wkb_builder.finish();

//         let result = euclidean_length(&wkb_array).unwrap();
//         assert_eq!(2, result.len());
//         assert_eq!(result.value(0), Euclidean.length(&linestring_1));
//         assert_eq!(result.value(1), Euclidean.length(&linestring_2));
//     }

//     #[test]
//     fn test_wkb_point() {
//         let mut wkb_builder: WkbBuilder<i32> =
//             geoarrow_array::builder::WkbBuilder::new(WkbType::new(Default::default()));
//         let point_1 = Point::new(1.0, 2.0);
//         let point_2 = Point::new(3.0, 4.0);
//         let _ = wkb_builder.push_geometry(Some(&point_1));
//         let _ = wkb_builder.push_geometry(Some(&point_2));
//         let wkb_array = wkb_builder.finish();

//         let result = euclidean_length(&wkb_array).unwrap();
//         assert_eq!(2, result.len());
//         assert_eq!(result.value(0), 0.0);
//         assert_eq!(result.value(1), 0.0);
//     }
// }
