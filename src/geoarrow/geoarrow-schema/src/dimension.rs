use std::{collections::HashSet, fmt::Display};

use arrow_schema::{ArrowError, Field, Fields};

use crate::error::{GeoArrowError, GeoArrowResult};

/// The dimension of the geometry array.
///
/// [Dimension] implements [TryFrom] for integers:
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    /// Two-dimensional.
    XY,

    /// Three-dimensional.
    XYZ,

    /// XYM (2D with measure).
    XYM,

    /// XYZM (3D with measure).
    XYZM,
}

impl Dimension {
    pub(crate) fn from_interleaved_field(field: &Field) -> GeoArrowResult<Self> {
        let dim = match field.name().as_str() {
            "xy" => Dimension::XY,
            "xyz" => Dimension::XYZ,
            "xym" => Dimension::XYM,
            "xyzm" => Dimension::XYZM,
            _ => {
                return Err(ArrowError::SchemaError(format!(
                    "Invalid interleaved field name: {}",
                    field.name()
                ))
                .into());
            }
        };
        Ok(dim)
    }

    pub(crate) fn from_separated_field(fields: &Fields) -> GeoArrowResult<Self> {
        let dim = if fields.len() == 2 {
            Self::XY
        } else if fields.len() == 3 {
            let field_names: HashSet<&str> =
                HashSet::from_iter(fields.iter().map(|f| f.name().as_str()));
            let xym_field_names = HashSet::<&str>::from_iter(["x", "y", "m"]);
            let xyz_field_names = HashSet::<&str>::from_iter(["x", "y", "z"]);

            if field_names.eq(&xym_field_names) {
                Self::XYM
            } else if field_names.eq(&xyz_field_names) {
                Self::XYZ
            } else {
                return Err(ArrowError::SchemaError(format!(
                    "Invalid field names for separated coordinates with 3 dimensions: {field_names:?}",

                ))
                .into());
            }
        } else if fields.len() == 4 {
            Self::XYZM
        } else {
            return Err(ArrowError::SchemaError(format!(
                "Invalid fields for separated coordinates: {fields:?}",
            ))
            .into());
        };
        Ok(dim)
    }

    /// Returns the number of dimensions.
    pub fn size(&self) -> usize {
        match self {
            Dimension::XY => 2,
            Dimension::XYZ => 3,
            Dimension::XYM => 3,
            Dimension::XYZM => 4,
        }
    }
}

impl From<Dimension> for geo_traits::Dimensions {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            Dimension::XYM => geo_traits::Dimensions::Xym,
            Dimension::XYZM => geo_traits::Dimensions::Xyzm,
        }
    }
}

impl TryFrom<geo_traits::Dimensions> for Dimension {
    type Error = GeoArrowError;

    fn try_from(value: geo_traits::Dimensions) -> std::result::Result<Self, Self::Error> {
        match value {
            geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => Ok(Dimension::XY),
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => Ok(Dimension::XYZ),
            geo_traits::Dimensions::Xym => Ok(Dimension::XYM),
            geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => {
                Ok(Dimension::XYZM)
            }
            _ => Err(GeoArrowError::InvalidGeoArrow(format!(
                "Unsupported dimension {value:?}"
            ))),
        }
    }
}

impl Display for Dimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dimension::XY => write!(f, "XY"),
            Dimension::XYZ => write!(f, "XYZ"),
            Dimension::XYM => write!(f, "XYM"),
            Dimension::XYZM => write!(f, "XYZM"),
        }
    }
}

// #[cfg(test)]
// mod test {
//     use std::iter::zip;

//     use arrow_schema::DataType;

//     use super::*;

//     #[test]
//     fn from_interleaved() {
//         assert!(matches!(
//             Dimension::from_interleaved_field(&Field::new("xy", DataType::Null, false)).unwrap(),
//             Dimension::XY
//         ));

//         assert!(matches!(
//             Dimension::from_interleaved_field(&Field::new("xyz", DataType::Null, false)).unwrap(),
//             Dimension::XYZ
//         ));

//         assert!(matches!(
//             Dimension::from_interleaved_field(&Field::new("xym", DataType::Null, false)).unwrap(),
//             Dimension::XYM
//         ));

//         assert!(matches!(
//             Dimension::from_interleaved_field(&Field::new("xyzm", DataType::Null, false)).unwrap(),
//             Dimension::XYZM
//         ));
//     }

//     #[test]
//     fn from_bad_interleaved() {
//         assert!(
//             Dimension::from_interleaved_field(&Field::new("banana", DataType::Null, false))
//                 .is_err()
//         );
//         assert!(
//             Dimension::from_interleaved_field(&Field::new("x", DataType::Null, false)).is_err()
//         );
//         assert!(
//             Dimension::from_interleaved_field(&Field::new("xyzmt", DataType::Null, false)).is_err()
//         );
//     }

//     fn test_fields(dims: &[&str]) -> Fields {
//         dims.iter()
//             .map(|dim| Field::new(*dim, DataType::Null, false))
//             .collect()
//     }

//     #[test]
//     fn from_separated() {
//         assert!(matches!(
//             Dimension::from_separated_field(&test_fields(&["x", "y"])).unwrap(),
//             Dimension::XY
//         ));

//         assert!(matches!(
//             Dimension::from_separated_field(&test_fields(&["x", "y", "z"])).unwrap(),
//             Dimension::XYZ
//         ));

//         assert!(matches!(
//             Dimension::from_separated_field(&test_fields(&["x", "y", "m"])).unwrap(),
//             Dimension::XYM
//         ));

//         assert!(matches!(
//             Dimension::from_separated_field(&test_fields(&["x", "y", "z", "m"])).unwrap(),
//             Dimension::XYZM
//         ));
//     }

//     #[test]
//     fn from_bad_separated() {
//         assert!(Dimension::from_separated_field(&test_fields(&["x"])).is_err());
//         assert!(Dimension::from_separated_field(&test_fields(&["x", "y", "a"])).is_err());
//         assert!(Dimension::from_separated_field(&test_fields(&["x", "y", "z", "m", "t"])).is_err());
//     }

//     #[test]
//     fn geotraits_dimensions() {
//         let geoarrow_dims = [
//             Dimension::XY,
//             Dimension::XYZ,
//             Dimension::XYM,
//             Dimension::XYZM,
//         ];
//         let geotraits_dims = [
//             geo_traits::Dimensions::Xy,
//             geo_traits::Dimensions::Xyz,
//             geo_traits::Dimensions::Xym,
//             geo_traits::Dimensions::Xyzm,
//         ];

//         for (geoarrow_dim, geotraits_dim) in zip(geoarrow_dims, geotraits_dims) {
//             let into_geotraits_dim: geo_traits::Dimensions = geoarrow_dim.into();
//             assert_eq!(into_geotraits_dim, geotraits_dim);

//             let into_geoarrow_dim: Dimension = geotraits_dim.try_into().unwrap();
//             assert_eq!(into_geoarrow_dim, geoarrow_dim);

//             assert_eq!(geoarrow_dim.size(), geotraits_dim.size());
//         }

//         let dims2: Dimension = geo_traits::Dimensions::Unknown(2).try_into().unwrap();
//         assert_eq!(dims2, Dimension::XY);

//         let dims3: Dimension = geo_traits::Dimensions::Unknown(3).try_into().unwrap();
//         assert_eq!(dims3, Dimension::XYZ);

//         let dims4: Dimension = geo_traits::Dimensions::Unknown(4).try_into().unwrap();
//         assert_eq!(dims4, Dimension::XYZM);

//         let dims_err: Result<Dimension, GeoArrowError> =
//             geo_traits::Dimensions::Unknown(0).try_into();
//         assert_eq!(
//             dims_err.unwrap_err().to_string(),
//             "Data not conforming to GeoArrow specification: Unsupported dimension Unknown(0)"
//         );
//     }
// }
