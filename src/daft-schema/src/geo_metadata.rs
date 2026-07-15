use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{dtype::DataType, schema::Schema};

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
        .filter(|n| only_columns.is_none_or(|only| only.contains(n)))
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

/// The GeoParquet spec's default CRS when the `crs` key is absent: lon/lat WGS84.
const DEFAULT_CRS: &str = "OGC:CRS84";

/// Cap `s` at `max` chars so a stray huge blob (e.g. an unrecognized PROJJSON
/// shape) never lands whole in a log line.
fn truncate_for_log(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}...")
    }
}

/// Render a PROJJSON `id.code` member (spec allows either a JSON string or a
/// number, e.g. `"CRS84"` or `4326`) as a plain string.
fn crs_id_code_to_string(code: &serde_json::Value) -> Option<String> {
    match code {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Classify a parsed `crs` JSON value: `None` if it is the GeoParquet default
/// (lon/lat WGS84), `Some(display)` with a compact identifier otherwise.
///
/// GeoParquet's SPEC form for `crs` is a full PROJJSON *object*, and real
/// writers (GeoPandas via `pyproj.CRS.to_json_dict()`) embed that object even
/// for the default WGS84 CRS. A PROJJSON CRS object carries an `id` member
/// like `{"authority": "OGC", "code": "CRS84"}`, so object-shaped values are
/// inspected for `id` rather than being flagged outright just for being an
/// object.
///
/// Recognized as default (not flagged):
/// - the string `"OGC:CRS84"`.
/// - an object whose `id` is `{authority: "OGC", code: "CRS84"}` (2D) or
///   `{authority: "OGC", code: "CRS84h"}` (3D).
///
/// NOTE (deliberate): `EPSG:4326` is WGS84 but is lat/lon axis order, while
/// `CRS84` (the GeoParquet default) is lon/lat. That's a genuine
/// silent-corruption risk, not a naming quirk, so EPSG:4326 — in either
/// string or PROJJSON-object form — is never folded into the default bucket
/// here. Do not "helpfully" special-case it as equivalent to CRS84.
///
/// When a non-default object CRS has an `id`, the display is the compact
/// `authority:code` (e.g. `"EPSG:3857"`) rather than the full PROJJSON blob.
/// Otherwise (or for any other JSON shape), the display falls back to a
/// truncated stringification so a malformed/unrecognized shape never dumps a
/// huge blob into a log line.
fn classify_crs(crs: &serde_json::Value) -> Option<String> {
    const MAX_LOG_LEN: usize = 80;
    match crs {
        serde_json::Value::String(s) => (s != DEFAULT_CRS).then(|| s.clone()),
        serde_json::Value::Object(_) => {
            let id = crs.get("id");
            let authority = id
                .and_then(|id| id.get("authority"))
                .and_then(|a| a.as_str());
            let code = id
                .and_then(|id| id.get("code"))
                .and_then(crs_id_code_to_string);
            if let (Some(authority), Some(code)) = (authority, &code)
                && authority == "OGC"
                && (code == "CRS84" || code == "CRS84h")
            {
                return None;
            }
            match (authority, code) {
                (Some(authority), Some(code)) => Some(format!("{authority}:{code}")),
                _ => Some(truncate_for_log(&crs.to_string(), MAX_LOG_LEN)),
            }
        }
        other => Some(truncate_for_log(&other.to_string(), MAX_LOG_LEN)),
    }
}

/// Returns `(column_name, crs)` for every WKB-encoded geometry column with a non-default CRS.
///
/// Considers columns in `geo_json` whose `crs` is present and is not the GeoParquet default
/// (`OGC:CRS84`, in either string or PROJJSON-object form — see
/// `classify_crs`). Lenient: returns empty on any parse failure, matching
/// `detect_geo_columns`.
///
/// Only WKB-encoded columns are considered (matching the encoding filter in
/// `detect_geo_columns`) — Daft never retypes natively-GeoArrow-encoded
/// columns to `Geometry`, so their CRS is irrelevant to Daft's readers.
/// Callers should additionally intersect the result with the columns they
/// actually detected/retyped (e.g. `detect_geo_columns`'s output), since this
/// function has no schema and can't confirm the column is present/read.
///
/// Daft's `Geometry` type has no CRS field. Planar ST_* defaults are CRS-agnostic
/// (results are in coordinate units), but the geodesic family (`use_spheroid`
/// variants, `great_circle_distance`, `st_geohash`, H3 helpers) assumes lon/lat
/// WGS84 — callers use this to warn instead of silently misinterpreting projected
/// coordinates.
pub fn non_default_crs_columns(geo_json: &str) -> Vec<(String, String)> {
    let Ok(meta) = serde_json::from_str::<GeoMetadata>(geo_json) else {
        return vec![];
    };
    meta.columns
        .into_iter()
        .filter(|(_, c)| c.encoding.eq_ignore_ascii_case("WKB"))
        .filter_map(|(name, c)| {
            let crs = c.crs?;
            classify_crs(&crs).map(|display| (name, display))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dtype::DataType, field::Field, schema::Schema};

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
        let arrow_geo =
            r#"{"version":"1.1.0","primary_column":"g","columns":{"g":{"encoding":"point"}}}"#;
        assert!(detect_geo_columns(arrow_geo, &read_schema).is_empty());
        // declared column absent from schema -> skipped
        let missing =
            r#"{"version":"1.1.0","primary_column":"x","columns":{"x":{"encoding":"WKB"}}}"#;
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

    #[test]
    fn non_default_crs_columns_flags_non_wgs84() {
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {"encoding": "WKB", "crs": "EPSG:3857"},
                "geom_wgs84": {"encoding": "WKB", "crs": "OGC:CRS84"},
                "geom_default": {"encoding": "WKB"}
            }
        }"#;
        let flagged = non_default_crs_columns(json);
        assert_eq!(flagged, vec![("geom".to_string(), "EPSG:3857".to_string())]);
    }

    #[test]
    fn non_default_crs_columns_empty_for_malformed_json() {
        assert!(non_default_crs_columns("{not json").is_empty());
    }

    #[test]
    fn non_default_crs_columns_handles_projjson_object() {
        // A GeoParquet file may legally carry a full PROJJSON object as `crs`
        // rather than a short string identifier. This must not panic and must
        // not be mistaken for the default CRS.
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {"encoding": "WKB", "crs": {"type": "GeographicCRS", "name": "Custom CRS"}}
            }
        }"#;
        let flagged = non_default_crs_columns(json);
        assert_eq!(flagged.len(), 1);
        assert_eq!(flagged[0].0, "geom");
        // stringified PROJJSON object, not mistaken for the default
        assert!(flagged[0].1.contains("Custom CRS"));
    }

    #[test]
    fn non_default_crs_columns_treats_projjson_crs84_object_as_default() {
        // GeoPandas (via pyproj.CRS.to_json_dict()) embeds a full PROJJSON object
        // even for the default WGS84 CRS. A PROJJSON object whose `id` member is
        // {authority: "OGC", code: "CRS84"} is the canonical GeoParquet default
        // and must NOT be flagged.
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "crs": {
                        "type": "GeographicCRS",
                        "name": "WGS 84 (CRS84)",
                        "id": {"authority": "OGC", "code": "CRS84"}
                    }
                }
            }
        }"#;
        assert!(non_default_crs_columns(json).is_empty());
    }

    #[test]
    fn non_default_crs_columns_flags_projjson_epsg3857_object_with_compact_id() {
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "crs": {
                        "type": "ProjectedCRS",
                        "name": "WGS 84 / Pseudo-Mercator",
                        "id": {"authority": "EPSG", "code": 3857}
                    }
                }
            }
        }"#;
        let flagged = non_default_crs_columns(json);
        assert_eq!(flagged, vec![("geom".to_string(), "EPSG:3857".to_string())]);
    }

    #[test]
    fn non_default_crs_columns_flags_epsg4326_object() {
        // EPSG:4326 is lat/lon axis order while CRS84 (the GeoParquet default) is
        // lon/lat. This is a genuine axis-order difference, not a naming quirk —
        // EPSG:4326 must stay flagged, deliberately, even though it is "WGS84".
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "crs": {
                        "type": "GeographicCRS",
                        "name": "WGS 84",
                        "id": {"authority": "EPSG", "code": 4326}
                    }
                }
            }
        }"#;
        let flagged = non_default_crs_columns(json);
        assert_eq!(flagged, vec![("geom".to_string(), "EPSG:4326".to_string())]);
    }

    #[test]
    fn non_default_crs_columns_skips_non_wkb_encoding() {
        // Daft only ever retypes WKB-encoded columns to Geometry (see
        // `detect_geo_columns`). A native-GeoArrow-encoded column (e.g. "point")
        // is never read as Geometry, so a non-default CRS on it is irrelevant
        // noise and must not be returned.
        let json = r#"{
            "version": "1.1.0",
            "primary_column": "geom",
            "columns": {
                "geom": {"encoding": "point", "crs": "EPSG:3857"}
            }
        }"#;
        assert!(non_default_crs_columns(json).is_empty());
    }
}
