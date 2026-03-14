use arrow_array::BooleanArray;
use geo::contains::Contains;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::{downcast::downcast_geoarrow_array_two_args, to_geo::geometry_to_geo};

pub fn contains(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<BooleanArray> {
    if left_array.len() != right_array.len() {
        Err(GeoArrowError::InvalidGeoArrow(
            "Arrays must have the same length".to_string(),
        ))
    } else {
        downcast_geoarrow_array_two_args!(left_array, right_array, _contains_impl)
    }
}

fn _contains_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanArray::builder(left_array.len());

    for (canidate_left, canidate_right) in left_array.iter().zip(right_array.iter()) {
        match (canidate_left, canidate_right) {
            (Some(left), Some(right)) => {
                let left_geom = geometry_to_geo(&left?)?;
                let right_geom = geometry_to_geo(&right?)?;
                let result = left_geom.contains(&right_geom);
                builder.append_value(result);
            }
            (_, _) => {
                builder.append_null();
            }
        }
    }
    Ok(builder.finish())
}

// #[cfg(test)]
// mod tests {
//     use geo::{Geometry, line_string, point, polygon};
//     use geoarrow_array::builder::GeometryBuilder;
//     use geoarrow_schema::{CoordType, GeometryType};

//     use super::*;

//     #[test]
//     fn test_contains() {
//         let test_pairs = [
//             //Right is contained in left
//             vec![
//                 Some(Geometry::from(polygon![
//                     (x: 1.0, y: 1.0),
//                     (x: 2.0, y: 1.0),
//                     (x: 2.0, y: 2.0),
//                     (x: 1.0, y: 2.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 1.5, y: 1.5),
//                     (x: 1.75, y: 1.5),
//                     (x: 1.75, y: 1.75),
//                     (x: 1.5, y: 1.75)
//                 ])),
//             ],
//             //Right is not contained in left
//             vec![
//                 Some(Geometry::from(polygon![
//                     (x: 1.0, y: 1.0),
//                     (x: 2.0, y: 1.0),
//                     (x: 2.0, y: 2.0),
//                     (x: 1.0, y: 2.0)
//                 ])),
//                 Some(Geometry::from(polygon![
//                     (x: 4.5, y: 4.5),
//                     (x: 5.5, y: 4.5),
//                     (x: 5.5, y: 5.5),
//                     (x: 3.5, y: 5.5),
//                 ])),
//             ],
//             //Mixed geometry
//             vec![
//                 Some(Geometry::from(line_string![
//                     (x: 0., y: 0.),
//                     (x: 2., y: 0.),
//                     (x: 2., y: 2.),
//                     (x: 0., y: 2.),
//                     (x: 0., y: 0.),
//                 ])),
//                 Some(Geometry::from(point!(x: 2., y: 0.))),
//             ],
//         ];

//         let geoms_left = test_pairs
//             .iter()
//             .map(|pair| pair[0].clone())
//             .collect::<Vec<_>>();
//         let geoms_right = test_pairs
//             .iter()
//             .map(|pair| pair[1].clone())
//             .collect::<Vec<_>>();

//         let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
//         let left_array = GeometryBuilder::from_nullable_geometries(&geoms_left, typ.clone())
//             .unwrap()
//             .finish();
//         let right_array = GeometryBuilder::from_nullable_geometries(&geoms_right, typ)
//             .unwrap()
//             .finish();

//         let result = contains(&left_array, &right_array).unwrap();
//         let expected = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);

//         assert_eq!(result, expected, "Contains test failed");
//     }

//     #[test]
//     #[should_panic(expected = "Arrays must have the same length")]
//     fn test_contains_length_mismatch() {
//         let geoms_left = vec![Some(Geometry::from(polygon![
//             (x: 1.0, y: 1.0),
//             (x: 2.0, y: 1.0),
//             (x: 2.0, y: 2.0),
//             (x: 1.0, y: 2.0)
//         ]))];
//         let geoms_right: Vec<Option<Geometry>> = vec![];

//         let typ = GeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
//         let left_array = GeometryBuilder::from_nullable_geometries(&geoms_left, typ.clone())
//             .unwrap()
//             .finish();
//         let right_array = GeometryBuilder::from_nullable_geometries(&geoms_right, typ)
//             .unwrap()
//             .finish();

//         contains(&left_array, &right_array).unwrap();
//     }
// }
