use std::sync::Arc;

use arrow_buffer::NullBufferBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{
        BinaryArray, BooleanArray, DataType, Field, Float64Array, FullNull, IntoSeries, Schema,
        Utf8Array,
    },
    series::Series,
};
use daft_dsl::{ExprRef, functions::FunctionArgs};
use geo::{Geometry, MultiPolygon};
use geo_traits::to_geo::ToGeoGeometry;

/// Encode a `geo::Geometry` into standard (ISO) little-endian WKB bytes.
pub fn geom_to_wkb(geom: &Geometry) -> DaftResult<Vec<u8>> {
    let mut buf = Vec::new();
    wkb::writer::write_geometry(&mut buf, geom, &wkb::writer::WriteOptions::default())
        .map_err(|e| DaftError::ComputeError(format!("WKB write error: {e:?}")))?;
    Ok(buf)
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared f64 literal helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Extract a required f64 arg from `FunctionArgs<Series>` by positional index or name.
///
/// Used by `st_buffer` to read its `distance` argument at eval time.
pub fn read_f64_arg(
    inputs: &FunctionArgs<Series>,
    pos: usize,
    name: &'static str,
    fn_name: &'static str,
) -> DaftResult<f64> {
    let s = inputs.required(pos)?;
    if s.len() == 1 {
        // Cast to Float64 and extract the scalar
        let casted = s.cast(&DataType::Float64)?;
        let arr = casted.f64()?;
        if arr.get(0).is_none() && arr.len() == 1 {
            // The value is present but null (e.g. lit(None))
            return Err(DaftError::ValueError(format!(
                "{fn_name}: {name} must not be null"
            )));
        }
        if let Some(v) = arr.get(0) {
            return Ok(v);
        }
    }
    Err(DaftError::ValueError(format!(
        "{fn_name}: {name} must be a numeric literal"
    )))
}

/// Extract a required f64 arg from `FunctionArgs<ExprRef>` by positional index or name.
///
/// Used at planning time (`get_return_field`) by `st_buffer`.
pub fn read_f64_arg_expr(
    inputs: &FunctionArgs<ExprRef>,
    pos: usize,
    name: &'static str,
    fn_name: &'static str,
) -> DaftResult<f64> {
    let expr = inputs.required(pos)?;
    expr.as_literal()
        .and_then(|l| l.as_f64().or_else(|| l.as_i64().map(|v| v as f64)))
        .ok_or_else(|| {
            DaftError::ValueError(format!("{fn_name}: {name} must be a numeric literal"))
        })
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared overlay operation helper
// ──────────────────────────────────────────────────────────────────────────────

/// Convert a Geometry to a MultiPolygon if it is a Polygon or MultiPolygon.
/// Returns None for non-polygon geometries.
pub(crate) fn as_multipolygon(g: &Geometry) -> Option<MultiPolygon> {
    match g {
        Geometry::Polygon(p) => Some(MultiPolygon(vec![p.clone()])),
        Geometry::MultiPolygon(mp) => Some(mp.clone()),
        _ => None,
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared use_spheroid helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Extract an optional boolean arg from `FunctionArgs<Series>` by positional index or name.
///
/// Used by `st_area`, `st_length`, and `st_distance` to read their `use_spheroid` argument.
pub fn read_bool_arg(
    inputs: &FunctionArgs<Series>,
    pos: usize,
    name: &'static str,
    fn_name: &'static str,
) -> DaftResult<bool> {
    let opt = inputs.optional((pos, name))?;
    match opt {
        None => Ok(false),
        Some(s) => {
            if s.data_type().is_boolean() && s.len() == 1 {
                Ok(s.bool().unwrap().get(0).unwrap_or(false))
            } else {
                Err(DaftError::ValueError(format!(
                    "{fn_name}: {name} must be a boolean literal"
                )))
            }
        }
    }
}

/// Extract an optional boolean arg from `FunctionArgs<ExprRef>` by positional index or name.
///
/// Used at planning time (`get_return_field`) by `st_area`, `st_length`, and `st_distance`.
pub fn read_bool_arg_expr(
    inputs: &FunctionArgs<ExprRef>,
    pos: usize,
    name: &'static str,
    fn_name: &'static str,
) -> DaftResult<bool> {
    let opt = inputs.optional((pos, name))?;
    match opt {
        None => Ok(false),
        Some(expr) => expr.as_literal().and_then(|l| l.as_bool()).ok_or_else(|| {
            DaftError::ValueError(format!("{fn_name}: {name} must be a boolean literal"))
        }),
    }
}

/// Strip EWKB SRID prefix so the standard `wkb` crate can parse it.
///
/// If the WKB type word has the PostGIS SRID flag (0x20000000) set, return a
/// new buffer with that flag cleared and the 4-byte SRID removed.  Otherwise
/// return a slice of the original bytes unchanged.
fn strip_ewkb_srid<'a>(bytes: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
    if bytes.len() < 5 {
        return std::borrow::Cow::Borrowed(bytes);
    }
    let le = bytes[0] == 1;
    let raw_type = if le {
        u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
    } else {
        u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])
    };
    if raw_type & 0x20000000 == 0 {
        return std::borrow::Cow::Borrowed(bytes);
    }
    // Clear the SRID flag and remove the 4-byte SRID field
    let clean_type = raw_type & !0x20000000;
    let clean_type_bytes = if le {
        clean_type.to_le_bytes()
    } else {
        clean_type.to_be_bytes()
    };
    let mut out = Vec::with_capacity(bytes.len() - 4);
    out.push(bytes[0]);
    out.extend_from_slice(&clean_type_bytes);
    // Skip bytes[5..9] (the SRID), then copy the rest
    if bytes.len() > 9 {
        out.extend_from_slice(&bytes[9..]);
    }
    std::borrow::Cow::Owned(out)
}

/// Parse WKB bytes into a `geo::Geometry`.
///
/// Accepts both standard ISO WKB and PostGIS EWKB (with SRID).
pub fn parse_wkb(bytes: &[u8]) -> DaftResult<Geometry> {
    let stripped = strip_ewkb_srid(bytes);
    let wkb = wkb::reader::read_wkb(stripped.as_ref())
        .map_err(|e| DaftError::ComputeError(format!("WKB parse error: {e:?}")))?;
    wkb.try_to_geometry()
        .ok_or_else(|| DaftError::ComputeError("WKB parse error: empty geometry".to_string()))
}

/// Get the physical BinaryArray backing a Geometry or Binary Series.
pub fn get_geometry_binary(series: &Series) -> DaftResult<&BinaryArray> {
    match series.data_type() {
        DataType::Geometry => Ok(&series.geometry()?.physical),
        DataType::Binary => series.binary(),
        _ => Err(DaftError::TypeError(format!(
            "Expected Geometry or Binary column, got {}",
            series.data_type()
        ))),
    }
}

/// Validate that a function argument at `arg_pos` is Geometry or Binary.
pub fn validate_geometry_field(
    inputs: &FunctionArgs<ExprRef>,
    schema: &Schema,
    arg_pos: usize,
    arg_name: &str,
    fn_name: &str,
) -> DaftResult<Field> {
    let f = inputs.required(arg_pos)?.to_field(schema)?;
    if !matches!(f.dtype, DataType::Geometry | DataType::Binary) {
        return Err(DaftError::TypeError(format!(
            "{fn_name}: {arg_name} must be Geometry or Binary (WKB), got {}",
            f.dtype
        )));
    }
    Ok(f)
}

// ──────────────────────────────────────────────────────────────────────────────
// Generic computation helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Apply a unary mapping over a geometry column → Float64 Series.
pub fn unary_geom_to_f64(
    series: &Series,
    out_name: &str,
    f: impl Fn(&Geometry) -> f64,
) -> DaftResult<Series> {
    let binary = get_geometry_binary(series)?;
    let len = binary.len();
    let mut values = Vec::with_capacity(len);
    let mut validity = NullBufferBuilder::new(len);

    for opt in binary.into_iter() {
        match opt.and_then(|b| parse_wkb(b).ok()) {
            Some(geom) => {
                values.push(f(&geom));
                validity.append_non_null();
            }
            None => {
                values.push(0.0);
                validity.append_null();
            }
        }
    }

    let field = Arc::new(Field::new(out_name, DataType::Float64));
    let arr = Float64Array::from_field_and_values(field, values).with_nulls(validity.finish())?;
    Ok(arr.into_series())
}

/// Apply a unary mapping over a geometry column → Boolean Series.
pub fn unary_geom_to_bool(
    series: &Series,
    out_name: &str,
    f: impl Fn(&Geometry) -> bool,
) -> DaftResult<Series> {
    let binary = get_geometry_binary(series)?;
    let len = binary.len();

    if len == 0 {
        return Ok(BooleanArray::empty(out_name, &DataType::Boolean).into_series());
    }

    let mut values: Vec<Option<bool>> = Vec::with_capacity(len);
    for opt in binary.into_iter() {
        values.push(opt.and_then(|b| parse_wkb(b).ok()).map(|g| f(&g)));
    }

    Ok(BooleanArray::from_iter(out_name, values.into_iter()).into_series())
}

/// Apply a unary mapping over a geometry column → Utf8 Series.
pub fn unary_geom_to_utf8(
    series: &Series,
    out_name: &str,
    f: impl Fn(&Geometry) -> String,
) -> DaftResult<Series> {
    let binary = get_geometry_binary(series)?;
    let values: Vec<Option<String>> = binary
        .into_iter()
        .map(|opt| opt.and_then(|b| parse_wkb(b).ok()).map(|g| f(&g)))
        .collect();

    Ok(Utf8Array::from_iter(out_name, values.iter().map(|v| v.as_deref())).into_series())
}

/// Wrap a `Vec<Option<Vec<u8>>>` of WKB bytes into a Geometry logical Series.
///
/// This is the shared tail used by `unary_geom_to_geom`, `binary_geom_to_geom`,
/// and `st_point` — any function that produces WKB-encoded geometries row-by-row.
pub(crate) fn wkb_opts_to_geometry_series(
    out_name: &str,
    wkb_values: Vec<Option<Vec<u8>>>,
) -> DaftResult<Series> {
    use daft_core::prelude::{GeometryType, LogicalArray};

    let len = wkb_values.len();
    let field = Field::new(out_name, DataType::Geometry);
    let mut builder = arrow_buffer::NullBufferBuilder::new(len);
    let phys_values: Vec<Option<&[u8]>> = wkb_values
        .iter()
        .map(|v| {
            if v.is_some() {
                builder.append_non_null();
            } else {
                builder.append_null();
            }
            v.as_deref()
        })
        .collect();

    let phys_field = Field::new(out_name, DataType::Binary);
    let phys_arr = BinaryArray::from_iter(&phys_field.name, phys_values.into_iter());
    let logical = LogicalArray::<GeometryType>::new(field, phys_arr.with_nulls(builder.finish())?);
    Ok(logical.into_series())
}

/// Apply a unary mapping over a geometry column → Geometry (WKB Binary) Series.
pub fn unary_geom_to_geom(
    series: &Series,
    out_name: &str,
    f: impl Fn(&Geometry) -> Option<Geometry>,
) -> DaftResult<Series> {
    let binary = get_geometry_binary(series)?;
    let len = binary.len();
    let mut wkb_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);

    for opt in binary.into_iter() {
        let result = opt
            .and_then(|b| parse_wkb(b).ok())
            .and_then(|g| f(&g))
            .and_then(|out_geom| geom_to_wkb(&out_geom).ok());
        wkb_values.push(result);
    }

    wkb_opts_to_geometry_series(out_name, wkb_values)
}

/// Apply a binary predicate over two geometry columns → Boolean Series.
pub fn binary_geom_to_bool(
    lhs: &Series,
    rhs: &Series,
    out_name: &str,
    f: impl Fn(&Geometry, &Geometry) -> bool,
) -> DaftResult<Series> {
    let lhs_bin = get_geometry_binary(lhs)?;
    let rhs_bin = get_geometry_binary(rhs)?;

    // support scalar broadcast: rhs may be length 1
    let rhs_scalar = rhs_bin.len() == 1;

    let rhs_geom_scalar: Option<Option<Geometry>> = if rhs_scalar {
        Some(
            rhs_bin
                .into_iter()
                .next()
                .flatten()
                .and_then(|b| parse_wkb(b).ok()),
        )
    } else {
        None
    };

    let values: Vec<Option<bool>> = if let Some(rhs_opt) = rhs_geom_scalar {
        lhs_bin
            .into_iter()
            .map(|lopt| {
                let lg = lopt.and_then(|b| parse_wkb(b).ok())?;
                let rg = rhs_opt.as_ref()?;
                Some(f(&lg, rg))
            })
            .collect()
    } else {
        lhs_bin
            .into_iter()
            .zip(rhs_bin.into_iter())
            .map(|(lopt, ropt)| {
                let lg = lopt.and_then(|b| parse_wkb(b).ok())?;
                let rg = ropt.and_then(|b| parse_wkb(b).ok())?;
                Some(f(&lg, &rg))
            })
            .collect()
    };

    Ok(BooleanArray::from_iter(out_name, values.into_iter()).into_series())
}

/// Apply a binary mapping over two geometry columns → Geometry (WKB) Series.
pub fn binary_geom_to_geom(
    lhs: &Series,
    rhs: &Series,
    out_name: &str,
    f: impl Fn(&Geometry, &Geometry) -> Option<Geometry>,
) -> DaftResult<Series> {
    let lhs_bin = get_geometry_binary(lhs)?;
    let rhs_bin = get_geometry_binary(rhs)?;

    // support scalar broadcast: rhs may be length 1
    let rhs_scalar = rhs_bin.len() == 1;

    let rhs_geom_scalar: Option<Option<Geometry>> = if rhs_scalar {
        Some(
            rhs_bin
                .into_iter()
                .next()
                .flatten()
                .and_then(|b| parse_wkb(b).ok()),
        )
    } else {
        None
    };

    let len = lhs_bin.len();
    let mut wkb_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);

    let compute = |lopt: Option<&[u8]>, rg: Option<&Geometry>| -> Option<Vec<u8>> {
        let lg = lopt.and_then(|b| parse_wkb(b).ok())?;
        let rg = rg?;
        f(&lg, rg).and_then(|g| geom_to_wkb(&g).ok())
    };

    if let Some(rhs_opt) = rhs_geom_scalar {
        for lopt in lhs_bin.into_iter() {
            wkb_values.push(compute(lopt, rhs_opt.as_ref()));
        }
    } else {
        for (lopt, ropt) in lhs_bin.into_iter().zip(rhs_bin.into_iter()) {
            let rg = ropt.and_then(|b| parse_wkb(b).ok());
            wkb_values.push(compute(lopt, rg.as_ref()));
        }
    }

    wkb_opts_to_geometry_series(out_name, wkb_values)
}

/// Apply a binary geometry → Float64 function (e.g. distance).
pub fn binary_geom_to_f64(
    lhs: &Series,
    rhs: &Series,
    out_name: &str,
    f: impl Fn(&Geometry, &Geometry) -> f64,
) -> DaftResult<Series> {
    let lhs_bin = get_geometry_binary(lhs)?;
    let rhs_bin = get_geometry_binary(rhs)?;
    let len = lhs_bin.len();
    let rhs_scalar = rhs_bin.len() == 1;

    let rhs_geom_scalar: Option<Option<Geometry>> = if rhs_scalar {
        Some(
            rhs_bin
                .into_iter()
                .next()
                .flatten()
                .and_then(|b| parse_wkb(b).ok()),
        )
    } else {
        None
    };

    let mut values = Vec::with_capacity(len);
    let mut validity = NullBufferBuilder::new(len);

    if let Some(rhs_opt) = rhs_geom_scalar {
        for lopt in lhs_bin.into_iter() {
            match (lopt.and_then(|b| parse_wkb(b).ok()), rhs_opt.as_ref()) {
                (Some(lg), Some(rg)) => {
                    values.push(f(&lg, &rg));
                    validity.append_non_null();
                }
                _ => {
                    values.push(0.0);
                    validity.append_null();
                }
            }
        }
    } else {
        for (lopt, ropt) in lhs_bin.into_iter().zip(rhs_bin.into_iter()) {
            match (
                lopt.and_then(|b| parse_wkb(b).ok()),
                ropt.and_then(|b| parse_wkb(b).ok()),
            ) {
                (Some(lg), Some(rg)) => {
                    values.push(f(&lg, &rg));
                    validity.append_non_null();
                }
                _ => {
                    values.push(0.0);
                    validity.append_null();
                }
            }
        }
    }

    let field = Arc::new(Field::new(out_name, DataType::Float64));
    let arr = Float64Array::from_field_and_values(field, values).with_nulls(validity.finish())?;
    Ok(arr.into_series())
}
