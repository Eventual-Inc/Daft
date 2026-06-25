use std::collections::BTreeMap;

use crate::dtype::DataType;
use crate::schema::Schema;
use serde::{Deserialize, Serialize};

const GEOPARQUET_VERSION: &str = "1.1.0";
pub const GEO_METADATA_KEY: &str = "geo";

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoMetadata {
    pub version: String,
    pub primary_column: String,
    pub columns: BTreeMap<String, GeoColumn>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoColumn {
    pub encoding: String,
    #[serde(default)]
    pub geometry_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crs: Option<serde_json::Value>,
    // Unknown spec fields (bbox/edges/orientation/...) are ignored on parse.
}

/// Build the GeoParquet `"geo"` metadata JSON for the Geometry columns of `schema`.
/// Returns None when there are no Geometry columns to declare.
///
/// `crs`, when supplied, is embedded as a JSON string identifier (e.g. "OGC:CRS84"); a full
/// PROJJSON object is not supported (Daft has no CRS concept). When `None`, `crs` is omitted,
/// which GeoParquet interprets as the default OGC:CRS84 (lon/lat WGS84).
pub fn build_geo_metadata(
    schema: &Schema,
    crs: Option<&str>,
    only_columns: Option<&[String]>,
) -> Option<String> {
    let geom_cols: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| f.dtype == DataType::Geometry)
        .map(|f| f.name.to_string())
        .filter(|n| only_columns.map_or(true, |only| only.contains(n)))
        .collect();
    if geom_cols.is_empty() {
        return None;
    }
    let crs_value = crs.map(|c| serde_json::Value::String(c.to_string()));
    let columns = geom_cols
        .iter()
        .map(|name| {
            (
                name.clone(),
                GeoColumn {
                    encoding: "WKB".to_string(),
                    geometry_types: vec![],
                    crs: crs_value.clone(),
                },
            )
        })
        .collect();
    let meta = GeoMetadata {
        version: GEOPARQUET_VERSION.to_string(),
        primary_column: geom_cols[0].clone(),
        columns,
    };
    serde_json::to_string(&meta).ok()
}

/// Parse a `"geo"` JSON and return the names of WKB-encoded geometry columns present as Binary in `schema`.
/// Lenient: returns empty on any parse failure (never panics).
///
/// Note: Daft has no separate `LargeBinary` dtype — `DataType::Binary` already maps to Arrow
/// `LargeBinary` at the Arrow level, so we match only `DataType::Binary` (and `DataType::Geometry`
/// for round-trip safety).
pub fn detect_geo_columns(geo_json: &str, schema: &Schema) -> Vec<String> {
    let Ok(meta) = serde_json::from_str::<GeoMetadata>(geo_json) else {
        return vec![];
    };
    meta.columns
        .into_iter()
        .filter(|(_, c)| c.encoding.eq_ignore_ascii_case("WKB"))
        .map(|(name, _)| name)
        .filter(|name| {
            matches!(
                schema.get_field(name).map(|f| &f.dtype),
                Ok(DataType::Binary) | Ok(DataType::Geometry)
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtype::DataType;
    use crate::field::Field;
    use crate::schema::Schema;

    fn schema_with_geo() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("geom", DataType::Geometry),
            Field::new("blob", DataType::Binary),
        ])
    }

    #[test]
    fn test_build_emits_conformant_geo_json() {
        let json = build_geo_metadata(&schema_with_geo(), None, None).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["version"], "1.1.0");
        assert_eq!(v["primary_column"], "geom");
        assert_eq!(v["columns"]["geom"]["encoding"], "WKB");
        // only the Geometry column is declared, not the plain Binary one
        assert!(v["columns"].get("blob").is_none());
        assert!(v["columns"].get("id").is_none());
    }

    #[test]
    fn test_build_none_when_no_geometry() {
        let s = Schema::new(vec![Field::new("id", DataType::Int64)]);
        assert!(build_geo_metadata(&s, None, None).is_none());
    }

    #[test]
    fn test_detect_roundtrips_build() {
        let json = build_geo_metadata(&schema_with_geo(), None, None).unwrap();
        // detect runs against a schema where geom arrived as Binary (pre-retype)
        let read_schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("geom", DataType::Binary),
        ]);
        assert_eq!(
            detect_geo_columns(&json, &read_schema),
            vec!["geom".to_string()]
        );
    }

    #[test]
    fn test_detect_lenient_and_wkb_only() {
        let read_schema = Schema::new(vec![Field::new("g", DataType::Binary)]);
        // malformed JSON -> empty
        assert!(detect_geo_columns("{not json", &read_schema).is_empty());
        // non-WKB encoding -> skipped
        let arrow_geo = r#"{"version":"1.1.0","primary_column":"g","columns":{"g":{"encoding":"point"}}}"#;
        assert!(detect_geo_columns(arrow_geo, &read_schema).is_empty());
        // declared column absent from schema -> skipped
        let missing = r#"{"version":"1.1.0","primary_column":"x","columns":{"x":{"encoding":"WKB"}}}"#;
        assert!(detect_geo_columns(missing, &read_schema).is_empty());
    }

    #[test]
    fn test_build_only_columns_filters_primary() {
        let s = Schema::new(vec![
            Field::new("a", DataType::Geometry),
            Field::new("b", DataType::Geometry),
        ]);
        let only = vec!["b".to_string()];
        let json = build_geo_metadata(&s, None, Some(&only)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["primary_column"], "b");
        assert!(v["columns"].get("a").is_none());
        assert_eq!(v["columns"]["b"]["encoding"], "WKB");
    }
}
