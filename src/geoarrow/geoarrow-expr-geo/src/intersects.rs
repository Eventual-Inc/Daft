use arrow_array::BooleanArray;
use geo::intersects::Intersects;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn intersects(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<BooleanArray> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Input arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _intersects_impl)
    }
}

fn _intersects_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanArray::builder(left_array.len());

    for (maybe_left, maybe_right) in left_array.iter().zip(right_array.iter()) {
        match (maybe_left, maybe_right) {
            (Some(left), Some(right)) => {
                let left_geom = geometry_to_geo(&left?)?;
                let right_geom = geometry_to_geo(&right?)?;
                let intersects = left_geom.intersects(&right_geom);
                builder.append_value(intersects);
            }
            _ => {
                // If either is null, the result is null
                builder.append_null();
            }
        }
    }

    Ok(builder.finish())
}

// #[cfg(test)]
// mod tests {
//     use geo::{Geometry, line_string, polygon};
//     use geoarrow_array::builder::GeometryBuilder;
//     use geoarrow_schema::{CoordType, GeometryType};

//     use super::*;

//     #[test]
//     fn test_intersects() {
//         // Group matching pairs for better visibility
//         let test_pairs = [
//             // Pair 1: Should intersect, overlapping unit squares
//             vec![
//                 Some(Geometry::from(polygon![
//                     (x: 1.0, y: 1.0),
//                     (x: 2.0, y: 1.0),
//                     (x: 2.0, y: 2.0),
//                     (x: 1.0, y: 2.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 1.5, y: 1.5),
//                     (x: 2.5, y: 1.5),
//                     (x: 2.5, y: 2.5),
//                     (x: 1.5, y: 2.5)
//                 ])),
//             ],
//             // Pair 2: Should not intersect, separated squares
//             vec![
//                 Some(Geometry::from(polygon![
//                     (x: 1.0, y: 1.0),
//                     (x: 2.0, y: 1.0),
//                     (x: 2.0, y: 2.0),
//                     (x: 1.0, y: 2.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 3.0, y: 3.0),
//                     (x: 4.0, y: 3.0),
//                     (x: 4.0, y: 4.0),
//                     (x: 3.0, y: 4.0)
//                 ])),
//             ],
//             // Pair 3: Should intersect, touching at corner
//             vec![
//                 Some(Geometry::from(polygon![
//                     (x: 2.0, y: 2.0),
//                     (x: 3.0, y: 2.0),
//                     (x: 3.0, y: 3.0),
//                     (x: 2.0, y: 3.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 3.0, y: 3.0),
//                     (x: 4.0, y: 3.0),
//                     (x: 4.0, y: 4.0),
//                     (x: 3.0, y: 4.0)
//                 ])),
//             ],
//             // Pair 4: Mixed geometry types, should intersect
//             vec![
//                 Some(Geometry::from(line_string! [
//                     (x: 1.0, y: 1.0),
//                     (x: 2.0, y: 2.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 1.5, y: 1.5),
//                     (x: 2.5, y: 1.5),
//                     (x: 2.5, y: 2.5),
//                     (x: 1.5, y: 2.5)
//                 ])),
//             ],
//             // Pair 5: Null geometries, should return null
//             vec![None, None],
//         ];

//         let geoms_left: Vec<_> = test_pairs.iter().map(|pair| pair[0].clone()).collect();
//         let geoms_right: Vec<_> = test_pairs.iter().map(|pair| pair[1].clone()).collect();

//         let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
//         let left_array = GeometryBuilder::from_nullable_geometries(&geoms_left, typ.clone())
//             .unwrap()
//             .finish();
//         let right_array = GeometryBuilder::from_nullable_geometries(&geoms_right, typ)
//             .unwrap()
//             .finish();

//         let result = intersects(&left_array, &right_array).unwrap();

//         let expected =
//             BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true), None]);

//         assert_eq!(result, expected);
//     }

//     #[test]
//     #[should_panic(expected = "Input arrays must have the same length")]
//     fn test_intersects_length_mismatch() {
//         let left_geom = vec![Some(Geometry::from(
//             polygon![(x: 0.0, y: 0.0), (x: 1.0, y: 0.0), (x: 1.0, y: 1.0), (x: 0.0, y: 1.0)],
//         ))];
//         let right_geom: Vec<Option<Geometry>> = vec![];

//         let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
//         let left_array = GeometryBuilder::from_nullable_geometries(&left_geom, typ.clone())
//             .unwrap()
//             .finish();
//         let right_array = GeometryBuilder::from_nullable_geometries(&right_geom, typ)
//             .unwrap()
//             .finish();

//         intersects(&left_array, &right_array).unwrap();
//     }

//     #[test]
//     fn test_intersects_empty_arrays() {
//         let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
//         let left_array =
//             GeometryBuilder::from_nullable_geometries(&Vec::<Option<Geometry>>::new(), typ.clone())
//                 .unwrap()
//                 .finish();
//         let right_array =
//             GeometryBuilder::from_nullable_geometries(&Vec::<Option<Geometry>>::new(), typ)
//                 .unwrap()
//                 .finish();

//         let result = intersects(&left_array, &right_array).unwrap();
//         assert_eq!(result.len(), 0);
//     }
// }
