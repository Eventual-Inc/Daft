//! Defines GeoArrow CRS metadata and CRS transforms used for writing GeoArrow data to file formats
//! that require different CRS representations.

use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{GeoArrowError, GeoArrowResult};

/// Coordinate Reference System information.
///
/// As of GeoArrow version 0.2, GeoArrow supports various CRS representations:
///
/// - A JSON object describing the coordinate reference system (CRS)
///   using [PROJJSON](https://proj.org/specifications/projjson.html).
/// - A string containing a serialized CRS representation. This option
///   is intended as a fallback for producers (e.g., database drivers or
///   file readers) that are provided a CRS in some form but do not have the
///   means to convert it to PROJJSON.
/// - Omitted, indicating that the producer does not have any information about
///   the CRS.
///
/// For maximum compatibility, producers should write PROJJSON.
///
/// Note that regardless of the axis order specified by the CRS, axis order will be interpreted
/// according to the wording in the [GeoPackage WKB binary
/// encoding](https://www.geopackage.org/spec130/index.html#gpb_format): axis order is always
/// (longitude, latitude) and (easting, northing) regardless of the the axis order encoded in the
/// CRS specification.
///
/// Note that [`PartialEq`] and [`Eq`] currently use their default, derived implementations, so
/// only `Crs` that are structurally exactly equal will compare as equal. Two different
/// representations of the same logical CRS will not compare as equal.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Crs {
    /// One of:
    ///
    /// - A JSON object describing the coordinate reference system (CRS)
    ///   using [PROJJSON](https://proj.org/specifications/projjson.html).
    /// - A string containing a serialized CRS representation. This option
    ///   is intended as a fallback for producers (e.g., database drivers or
    ///   file readers) that are provided a CRS in some form but do not have the
    ///   means to convert it to PROJJSON.
    /// - Omitted, indicating that the producer does not have any information about
    ///   the CRS.
    ///
    /// For maximum compatibility, producers should write PROJJSON where possible.
    /// Note that regardless of the axis order specified by the CRS, axis order will be interpreted
    /// [GeoPackage WKB binary encoding](https://www.geopackage.org/spec130/index.html#gpb_format):
    /// axis order is always (longitude, latitude) and (easting, northing)
    /// regardless of the the axis order encoded in the CRS specification.
    crs: Option<Value>,

    /// An optional string disambiguating the value of the `crs` field.
    ///
    /// The `"crs_type"` should be omitted if the producer cannot guarantee the validity
    /// of any of the above values (e.g., if it just serialized a CRS object
    /// specifically into one of these representations).
    #[serde(skip_serializing_if = "Option::is_none")]
    crs_type: Option<CrsType>,
}

impl Crs {
    /// Construct from a PROJJSON object.
    ///
    /// Note that `value` should be a _parsed_ JSON object; this should not contain
    /// `Value::String`.
    pub fn from_projjson(value: Value) -> Self {
        Self {
            crs: Some(value),
            crs_type: Some(CrsType::Projjson),
        }
    }

    /// Construct from a WKT:2019 string.
    pub fn from_wkt2_2019(value: String) -> Self {
        Self {
            crs: Some(Value::String(value)),
            crs_type: Some(CrsType::Wkt2_2019),
        }
    }

    /// Construct from an opaque string.
    pub fn from_unknown_crs_type(value: String) -> Self {
        Self {
            crs: Some(Value::String(value)),
            crs_type: None,
        }
    }

    /// Construct from an authority:code string.
    pub fn from_authority_code(value: String) -> Self {
        assert!(value.contains(':'), "':' should be authority:code CRS");
        Self {
            crs: Some(Value::String(value)),
            crs_type: Some(CrsType::AuthorityCode),
        }
    }

    /// Construct from an opaque string identifier
    pub fn from_srid(value: String) -> Self {
        Self {
            crs: Some(Value::String(value)),
            crs_type: Some(CrsType::Srid),
        }
    }

    /// Access the underlying [CrsType].
    pub fn crs_type(&self) -> Option<CrsType> {
        self.crs_type
    }

    /// Access the underlying CRS value.
    ///
    /// The return value is one of:
    ///
    /// - A JSON object ([`Value::Object`]) describing the coordinate reference system (CRS)
    ///   using [PROJJSON](https://proj.org/specifications/projjson.html).
    /// - A string ([`Value::String`]) containing a serialized CRS representation. This option
    ///   is intended as a fallback for producers (e.g., database drivers or
    ///   file readers) that are provided a CRS in some form but do not have the
    ///   means to convert it to PROJJSON.
    /// - Omitted, indicating that the producer does not have any information about
    ///   the CRS.
    ///
    /// Consult [`crs_type`][Self::crs_type] to accurately determine the CRS type.
    pub fn crs_value(&self) -> Option<&Value> {
        self.crs.as_ref()
    }

    /// Return `true` if we should include a CRS key in the GeoArrow metadata
    pub(crate) fn should_serialize(&self) -> bool {
        self.crs.is_some()
    }
}

/// An optional string disambiguating the value of the `crs` field.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CrsType {
    /// Indicates that the `"crs"` field was written as
    /// [PROJJSON](https://proj.org/specifications/projjson.html).
    #[serde(rename = "projjson")]
    Projjson,

    /// Indicates that the `"crs"` field was written as
    /// [WKT2:2019](https://www.ogc.org/publications/standard/wkt-crs/).
    #[serde(rename = "wkt2:2019")]
    Wkt2_2019,

    /// Indicates that the `"crs"` field contains an identifier
    /// in the form `AUTHORITY:CODE`. This should only be used as a last resort
    /// (i.e., producers should prefer writing a complete description of the CRS).
    #[serde(rename = "authority_code")]
    AuthorityCode,

    /// Indicates that the `"crs"` field contains an opaque identifier
    /// that requires the consumer to communicate with the producer outside of
    /// this metadata. This should only be used as a last resort for database
    /// drivers or readers that have no other option.
    #[serde(rename = "srid")]
    Srid,
}

/// CRS transforms used for writing GeoArrow data to file formats that require different CRS
/// representations.
pub trait CrsTransform: Debug {
    /// Convert the CRS contained in this Metadata to a PROJJSON object.
    ///
    /// Users should prefer calling `extract_projjson`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_projjson(&self, crs: &Crs) -> GeoArrowResult<Option<Value>>;

    /// Convert the CRS contained in this Metadata to a WKT string.
    ///
    /// Users should prefer calling `extract_wkt`, which will first unwrap the underlying
    /// array metadata if it's already PROJJSON.
    fn _convert_to_wkt(&self, crs: &Crs) -> GeoArrowResult<Option<String>>;

    /// Extract PROJJSON from the provided metadata.
    ///
    /// If the CRS is already stored as PROJJSON, this will return that. Otherwise it will call
    /// [`Self::_convert_to_projjson`].
    fn extract_projjson(&self, crs: &Crs) -> GeoArrowResult<Option<Value>> {
        match crs.crs_type() {
            Some(CrsType::Projjson) => Ok(crs.crs_value().cloned()),
            _ => self._convert_to_projjson(crs),
        }
    }

    /// Extract WKT from the provided metadata.
    ///
    /// If the CRS is already stored as WKT, this will return that. Otherwise it will call
    /// [`Self::_convert_to_wkt`].
    #[allow(clippy::collapsible_if)]
    fn extract_wkt(&self, crs: &Crs) -> GeoArrowResult<Option<String>> {
        if let (Some(crs), Some(crs_type)) = (crs.crs_value(), crs.crs_type()) {
            if crs_type == CrsType::Wkt2_2019 {
                if let Value::String(inner) = crs {
                    return Ok::<_, GeoArrowError>(Some(inner.clone()));
                }
            }
        }

        self._convert_to_wkt(crs)
    }
}

/// A default implementation for [CrsTransform] which does not do any CRS conversion.
///
/// Instead of raising an error, this will **silently drop any CRS information when writing data**.
#[derive(Debug, Clone, Default)]
pub struct DefaultCrsTransform {}

impl CrsTransform for DefaultCrsTransform {
    fn _convert_to_projjson(&self, _crs: &Crs) -> GeoArrowResult<Option<Value>> {
        // Unable to convert CRS to PROJJSON
        // So we proceed with missing CRS
        // TODO: we should probably log this.
        Ok(None)
    }

    fn _convert_to_wkt(&self, _crs: &Crs) -> GeoArrowResult<Option<String>> {
        // Unable to convert CRS to WKT
        // So we proceed with missing CRS
        // TODO: we should probably log this.
        Ok(None)
    }
}

// #[cfg(test)]
// mod test {
//     use serde_json::json;

//     use super::*;

//     #[test]
//     fn crs_omitted() {
//         let crs = Crs::default();
//         assert!(crs.crs_value().is_none());
//         assert!(crs.crs_type().is_none());
//         assert!(!crs.should_serialize());
//     }

//     #[test]
//     fn crs_projjson() {
//         let crs = Crs::from_projjson(json!({}));
//         assert!(crs.crs_value().is_some_and(|x| x.is_object()));
//         assert!(
//             crs.crs_type()
//                 .is_some_and(|x| matches!(x, CrsType::Projjson))
//         );
//         assert!(crs.should_serialize());
//         assert_eq!(
//             serde_json::to_string(&crs).unwrap(),
//             r#"{"crs":{},"crs_type":"projjson"}"#
//         );
//     }

//     #[test]
//     fn crs_wkt2() {
//         let crs = Crs::from_wkt2_2019("TESTCRS[]".to_string());
//         assert_eq!(
//             crs.crs_value(),
//             Some(&Value::String("TESTCRS[]".to_string()))
//         );
//         assert!(matches!(crs.crs_type(), Some(CrsType::Wkt2_2019)));
//         assert!(crs.should_serialize());
//         assert_eq!(
//             serde_json::to_string(&crs).unwrap(),
//             r#"{"crs":"TESTCRS[]","crs_type":"wkt2:2019"}"#
//         );
//     }

//     #[test]
//     fn crs_authority_code() {
//         let crs = Crs::from_authority_code("GEOARROW:1234".to_string());
//         assert_eq!(
//             crs.crs_value(),
//             Some(&Value::String("GEOARROW:1234".to_string()))
//         );
//         assert!(matches!(crs.crs_type(), Some(CrsType::AuthorityCode)));
//         assert!(crs.should_serialize());
//         assert_eq!(
//             serde_json::to_string(&crs).unwrap(),
//             r#"{"crs":"GEOARROW:1234","crs_type":"authority_code"}"#
//         );
//     }

//     #[test]
//     fn crs_srid() {
//         let crs = Crs::from_srid("1234".to_string());
//         assert_eq!(crs.crs_value(), Some(&Value::String("1234".to_string())),);
//         assert!(matches!(crs.crs_type(), Some(CrsType::Srid)));
//         assert!(crs.should_serialize());
//         assert_eq!(
//             serde_json::to_string(&crs).unwrap(),
//             r#"{"crs":"1234","crs_type":"srid"}"#
//         );
//     }

//     #[test]
//     fn crs_unknown() {
//         let crs = Crs::from_unknown_crs_type("1234".to_string());
//         assert_eq!(crs.crs_value(), Some(&Value::String("1234".to_string())),);
//         assert!(crs.crs_type().is_none());
//         assert!(crs.should_serialize());
//         assert_eq!(serde_json::to_string(&crs).unwrap(), r#"{"crs":"1234"}"#);
//     }
// }
