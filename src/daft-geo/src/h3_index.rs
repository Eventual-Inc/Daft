//! H3-based spatial index utilities (v1 index format).
//!
//! Provides cell-set computation for WKB geometries used by the
//! `SpatialPartitionPruning` optimizer rule when it loads a v1
//! `_spatial_index.idx` sidecar.
//!
//! # Design
//!
//! Each parquet file's spatial coverage is stored as a **set of H3 cell
//! indices** rather than a single MBR.  H3 hexagons tile the sphere at
//! uniform resolution, so two files whose MBRs overlap heavily can still
//! have *disjoint* H3 cell sets — enabling far more aggressive pruning.
//!
//! At query time the optimizer:
//! 1. Computes the H3 cells that cover the query geometry.
//! 2. Keeps only files whose stored cell set intersects the query cells.
//!
//! # Coverage strategy (for query geometries in Rust)
//!
//! * **Points** — the single containing cell.
//! * **Other geometries** — vertex cells (+ k=1 ring for boundary safety)
//!   plus a bbox grid sample to catch interior cells.  This is conservative:
//!   it may include a few extra cells, but never misses a cell the geometry
//!   truly occupies (no false negatives in pruning).

use std::collections::HashSet;

use geo::{BoundingRect, CoordsIter, Geometry};
use h3o::{CellIndex, LatLng, Resolution};

// ── Public API ────────────────────────────────────────────────────────────

/// Parse a slice of H3 hex-string cell IDs (e.g. `"85283473fffffff"`) into
/// their `u64` representation.  Invalid strings are silently skipped.
pub fn parse_h3_cells(cell_strs: &[String]) -> Vec<u64> {
    cell_strs
        .iter()
        .filter_map(|s| s.parse::<CellIndex>().ok().map(u64::from))
        .collect()
}

/// Convert a WKB geometry to a conservative set of H3 cell indices (as `u64`).
///
/// Returns `None` when the WKB is invalid or the resolution is out of range.
pub fn wkb_to_h3_cells(wkb: &[u8], resolution: u8) -> Option<HashSet<u64>> {
    let geom = crate::utils::parse_wkb(wkb).ok()?;
    let res = Resolution::try_from(resolution).ok()?;
    Some(geometry_to_h3_cells(&geom, res))
}

/// Return `true` when the two H3 cell lists share at least one common cell.
///
/// Builds a `HashSet` from the smaller list to keep the lookup O(n + m).
pub fn h3_cells_intersect(file_cells: &[u64], query_cells: &[u64]) -> bool {
    if file_cells.is_empty() || query_cells.is_empty() {
        return false;
    }
    if file_cells.len() <= query_cells.len() {
        let set: HashSet<u64> = file_cells.iter().copied().collect();
        query_cells.iter().any(|c| set.contains(c))
    } else {
        let set: HashSet<u64> = query_cells.iter().copied().collect();
        file_cells.iter().any(|c| set.contains(c))
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────

fn geometry_to_h3_cells(geom: &Geometry<f64>, res: Resolution) -> HashSet<u64> {
    let mut cells: HashSet<u64> = HashSet::new();

    // Points: single exact cell.
    if let Geometry::Point(p) = geom {
        if let Ok(ll) = LatLng::new(p.y(), p.x()) {
            cells.insert(u64::from(ll.to_cell(res)));
        }
        return cells;
    }

    // All other geometry types:
    // Step 1 — vertex cells + k=1 ring (covers polygon boundary edges).
    for coord in geom.coords_iter() {
        if let Ok(ll) = LatLng::new(coord.y, coord.x) {
            let cell = ll.to_cell(res);
            cells.insert(u64::from(cell));
            for n in cell.grid_disk::<Vec<CellIndex>>(1) {
                cells.insert(u64::from(n));
            }
        }
    }

    // Step 2 — bounding-box grid sample (catches interior cells for wide polygons).
    if let Some(bbox) = geom.bounding_rect() {
        let step = h3_step_deg(res as u8);
        let y_steps = (((bbox.max().y + step - bbox.min().y) / step).ceil() as i64).max(0);
        let x_steps = (((bbox.max().x + step - bbox.min().x) / step).ceil() as i64).max(0);
        for iy in 0..=y_steps {
            let y = (iy as f64).mul_add(step, bbox.min().y);
            for ix in 0..=x_steps {
                let x = (ix as f64).mul_add(step, bbox.min().x);
                if let Ok(ll) = LatLng::new(
                    y.clamp(-89.9_f64, 89.9_f64),
                    x.clamp(-179.9_f64, 179.9_f64),
                ) {
                    cells.insert(u64::from(ll.to_cell(res)));
                }
            }
        }
    }

    cells
}

/// Grid-sampling step in degrees for bounding-box coverage at a given
/// H3 resolution.  Set to ~75 % of the cell edge length so that even the
/// tightest hex geometry is covered by at least one sample point.
///
/// Edge lengths sourced from the official H3 documentation (average km).
fn h3_step_deg(res: u8) -> f64 {
    // edge_km / 111.32 km/° × 0.75
    const STEPS: [f64; 16] = [
        7.46,       // res 0: 1107.71 km
        2.82,       // res 1:  418.68 km
        1.07,       // res 2:  158.24 km
        0.403,      // res 3:   59.81 km
        0.152,      // res 4:   22.61 km
        0.0575,     // res 5:    8.544 km
        0.0218,     // res 6:    3.229 km
        0.00822,    // res 7:    1.220 km
        0.00311,    // res 8:    0.4614 km
        0.00118,    // res 9:    0.1745 km
        0.000444,   // res 10:   0.0660 km
        0.000168,   // res 11:   0.0250 km
        0.0000634,  // res 12:   0.00946 km
        0.0000240,  // res 13:   0.00358 km
        0.00000907, // res 14:   0.00135 km
        0.00000343, // res 15:   0.000511 km
    ];
    STEPS[(res as usize).min(15)]
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn point_wkb(x: f64, y: f64) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_all(&[1u8]).unwrap();
        buf.write_all(&1u32.to_le_bytes()).unwrap();
        buf.write_all(&x.to_le_bytes()).unwrap();
        buf.write_all(&y.to_le_bytes()).unwrap();
        buf
    }

    #[test]
    fn point_returns_single_cell() {
        let wkb = point_wkb(2.35, 48.85); // near Paris
        let cells = wkb_to_h3_cells(&wkb, 5).unwrap();
        assert_eq!(cells.len(), 1);
    }

    #[test]
    fn intersect_disjoint_is_false() {
        // Paris and Sydney are far apart — their cells should not intersect.
        let paris = wkb_to_h3_cells(&point_wkb(2.35, 48.85), 5).unwrap();
        let sydney = wkb_to_h3_cells(&point_wkb(151.2, -33.87), 5).unwrap();
        let p: Vec<u64> = paris.into_iter().collect();
        let s: Vec<u64> = sydney.into_iter().collect();
        assert!(!h3_cells_intersect(&p, &s));
    }

    #[test]
    fn same_point_intersects() {
        let cells = wkb_to_h3_cells(&point_wkb(0.0, 51.5), 5)
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert!(h3_cells_intersect(&cells, &cells));
    }

    #[test]
    fn parse_roundtrip() {
        let wkb = point_wkb(13.4, 52.5); // Berlin
        let cells = wkb_to_h3_cells(&wkb, 6).unwrap();
        // Encode as hex strings and round-trip through parse_h3_cells.
        let strs: Vec<String> = cells
            .iter()
            .map(|&n| {
                let c = CellIndex::try_from(n).unwrap();
                c.to_string()
            })
            .collect();
        let parsed = parse_h3_cells(&strs);
        let parsed_set: HashSet<u64> = parsed.into_iter().collect();
        assert_eq!(cells, parsed_set);
    }
}
