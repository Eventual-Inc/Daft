use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BoundingRect, Centroid, Geometry, Intersects};
use geohash::{Coord as GeohashCoord, encode};
use serde::{Deserialize, Serialize};

use crate::utils::{get_geometry_binary, parse_wkb, read_f64_arg, validate_geometry_field};

/// Compute geohash of the centroid of a geometry.
fn geom_geohash(g: &Geometry, precision: usize) -> Option<String> {
    let centroid = g.centroid()?;
    let coord = GeohashCoord {
        x: centroid.x(),
        y: centroid.y(),
    };
    encode(coord, precision).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohash {
    pub precision: u8,
}

#[typetag::serde]
impl ScalarUDF for StGeohash {
    fn name(&self) -> &'static str {
        "st_geohash"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        // Read precision from the function arguments (Python path). Fall back to
        // `self.precision` for the Rust-API path where only `geom` is in `inputs`.
        let precision = read_f64_arg(&inputs, 1, "precision", self.name())
            .map(|v| v as usize)
            .unwrap_or(self.precision as usize);
        let binary = get_geometry_binary(inputs.required(0)?)?;

        let values: Vec<Option<String>> = binary
            .into_iter()
            .map(|opt| {
                opt.and_then(|b| parse_wkb(b).ok())
                    .and_then(|g| geom_geohash(&g, precision))
            })
            .collect();

        Ok(Utf8Array::from_iter(self.name(), values.iter().map(|v| v.as_deref())).into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geohash string of a geometry's centroid at the given precision (1-12)."
    }
}

#[must_use]
pub fn st_geohash(geom: ExprRef, precision: u8) -> ExprRef {
    ScalarFn::builtin(StGeohash { precision }, vec![geom]).into()
}

// ──────────────────────────────────────────────────────────────────────────────
// Geohash bounding-box coverage: returns geohash prefixes that cover a bbox
// ──────────────────────────────────────────────────────────────────────────────

/// Hard cap on the number of geohash cells the flood fill in
/// `collect_covering_cells` will collect before giving up.
///
/// A query geometry with a large bounding box (e.g. a continent-sized
/// polygon) can overlap millions of geohash cells at typical precisions
/// (~1M cells for a 60°×30° bbox at precision 5, tens of millions near
/// global extent). Enumerating them all as `String`s at *plan time* — before
/// any data is read — can stall query planning on ordinary user input.
///
/// The covering set is only ever used by `GeohashPruning` as a purely
/// optional pruning aid, which already treats an empty covering set as "do
/// not prune" (see `geom.is_empty()` check in
/// `daft-logical-plan::optimization::rules::geohash_pruning`). So instead of
/// truncating (which would be UNSOUND — a partial covering set would prune
/// away rows whose true cells were dropped), we abort the flood entirely and
/// return an empty set once the cap is hit, which safely disables pruning
/// for oversized query geometries while preserving correctness.
///
/// 4096 is comfortably above the cell counts produced by realistic
/// filter geometries (neighborhoods, cities, even large metro areas) at the
/// precisions typically used for geohash columns, while being small enough
/// that hitting the cap costs microseconds, not seconds.
const MAX_COVERING_CELLS: usize = 4096;

/// Compute all geohash cells at `precision` that overlap with the given geometry's bounding box.
/// Used for automatic geohash-based partition pruning.
///
/// Returns an empty `Vec` both when the geometry has no usable bounding box
/// and when the true covering set would exceed `MAX_COVERING_CELLS` — in
/// both cases the caller (`GeohashPruning`) treats an empty result as "do
/// not prune", which is always a safe, if less optimized, outcome.
pub fn geohash_covers_geometry(g: &Geometry, precision: usize) -> Vec<String> {
    let bbox = match g.bounding_rect() {
        Some(b) => b,
        None => {
            if let Some(c) = g.centroid() {
                let coord = GeohashCoord { x: c.x(), y: c.y() };
                return encode(coord, precision).ok().into_iter().collect();
            }
            return vec![];
        }
    };

    let Ok(start_hash) = geohash::encode(
        GeohashCoord {
            x: bbox.min().x,
            y: bbox.min().y,
        },
        precision,
    ) else {
        return vec![];
    };

    let mut cells = std::collections::HashSet::new();
    let capped = collect_covering_cells(&mut cells, &start_hash, &bbox, precision);
    if capped {
        // The covering set is larger than we're willing to enumerate. Never
        // return a partial/truncated set (that would be unsound — it would
        // cause the pruning rule to drop rows whose true cells were never
        // collected). Returning empty is always safe: it just disables
        // pruning for this query geometry.
        return vec![];
    }
    cells.into_iter().collect()
}

/// Flood-fill outward from `start` (a cell known to overlap `bbox`), continuing
/// only into neighbor cells whose own decoded bounding rectangle intersects
/// `bbox`. Complete: the cells overlapping an axis-aligned bbox form a single
/// 8-connected region, all reachable from any member. Tight and terminating:
/// the flood never leaves the bbox's finite cell set. (The previous
/// implementation broke on dequeuing the max-corner cell, losing same-layer
/// cells still in the queue, and enqueued neighbors unfiltered, flooding an
/// O(D²) disk around the start.)
///
/// `cells` doubles as both the result set and the seen/visited check: a cell
/// is inserted into `cells` the moment it is enqueued (not when it is later
/// dequeued), so a cell already present in `cells` has already been queued
/// and must not be enqueued again. Every enqueued cell is eventually
/// dequeued and processed, so on a complete (uncapped) run the final
/// contents of `cells` are exactly the same as if it had instead been
/// populated at dequeue time — the intermediate bookkeeping is just cheaper,
/// since it avoids keeping a second `String` set with the same contents.
///
/// Bounded: the flood aborts once enqueuing another cell would grow `cells`
/// past `MAX_COVERING_CELLS`, returning `true` ("capped"). Callers must NOT
/// treat `cells` as a valid (partial) covering set when this returns `true`
/// — the flood stops mid-BFS, so `cells` at that point is an arbitrary,
/// incomplete prefix of the true covering set, not a sound approximation of
/// it. (`geohash_covers_geometry` never reads `cells` when `capped` is
/// `true`, so this partial content is never observed by callers.)
fn collect_covering_cells(
    cells: &mut std::collections::HashSet<String>,
    start: &str,
    bbox: &geo::Rect<f64>,
    precision: usize,
) -> bool {
    if start.is_empty() {
        return false;
    }
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(start.to_string());
    cells.insert(start.to_string());

    while let Some(current) = queue.pop_front() {
        let Ok(neighbors) = geohash::neighbors(&current) else {
            continue;
        };
        for neighbor in [
            neighbors.n,
            neighbors.ne,
            neighbors.e,
            neighbors.se,
            neighbors.s,
            neighbors.sw,
            neighbors.w,
            neighbors.nw,
        ] {
            if neighbor.len() != precision || cells.contains(&neighbor) {
                continue;
            }
            let Ok(neighbor_bbox) = geohash::decode_bbox(&neighbor) else {
                continue;
            };
            if !neighbor_bbox.intersects(bbox) {
                continue;
            }
            if cells.len() >= MAX_COVERING_CELLS {
                return true;
            }
            cells.insert(neighbor.clone());
            queue.push_back(neighbor);
        }
    }
    false
}

/// Check if a geohash string is covered by any of the given covering cells.
/// Used as a fast pre-filter before exact spatial evaluation.
pub fn geohash_in_covering_cells(hash: &str, covering: &[String]) -> bool {
    covering
        .iter()
        .any(|c| hash.starts_with(c.as_str()) || c.starts_with(hash))
}

#[cfg(test)]
mod covering_tests {
    use std::collections::HashSet;

    use geo::{Geometry, Intersects, Rect, coord};

    use super::*;

    #[test]
    fn covering_includes_all_cells_of_a_2x2_bbox() {
        // Straddles the precision-5 cell corner at 0.0439453125°, spanning 2x2 cells.
        // The old corner-hit BFS dequeues the NE (end) cell before the SE cell and
        // breaks with SE still in the queue — SE is silently missing.
        let rect = Rect::new(coord! { x: 0.02, y: 0.02 }, coord! { x: 0.06, y: 0.06 });
        let cells = geohash_covers_geometry(&Geometry::Rect(rect), 5);
        let got: HashSet<String> = cells.into_iter().collect();
        for (x, y) in [(0.02, 0.02), (0.06, 0.02), (0.02, 0.06), (0.06, 0.06)] {
            let h = geohash::encode(GeohashCoord { x, y }, 5).unwrap();
            assert!(
                got.contains(&h),
                "missing cell {h} containing bbox corner ({x},{y})"
            );
        }
    }

    #[test]
    fn covering_includes_all_cells_of_a_wide_bbox() {
        // Multi-cell in both axes at precision 3: every cell a swept point hashes to
        // must be in the covering set.
        let rect = Rect::new(coord! { x: -10.0, y: -10.0 }, coord! { x: 10.0, y: 10.0 });
        let cells = geohash_covers_geometry(&Geometry::Rect(rect), 3);
        let got: HashSet<String> = cells.into_iter().collect();
        let mut x = -10.0;
        while x <= 10.0 {
            let mut y = -10.0;
            while y <= 10.0 {
                let h = geohash::encode(GeohashCoord { x, y }, 3).unwrap();
                assert!(got.contains(&h), "missing cell {h} for point ({x},{y})");
                y += 0.5;
            }
            x += 0.5;
        }
    }

    #[test]
    fn covering_cells_all_intersect_the_bbox() {
        // Tightness: the old BFS flooded an O(D²) disk far outside the bbox.
        let rect = Rect::new(coord! { x: -1.0, y: -1.0 }, coord! { x: 1.0, y: 1.0 });
        let cells = geohash_covers_geometry(&Geometry::Rect(rect), 4);
        assert!(!cells.is_empty());
        for cell in &cells {
            let cell_bbox = geohash::decode_bbox(cell).unwrap();
            assert!(
                cell_bbox.intersects(&rect),
                "cell {cell} does not intersect the query bbox"
            );
        }
    }

    #[test]
    fn covering_set_is_empty_when_query_bbox_is_enormous() {
        // At precision 5, a near-global bbox overlaps tens of millions of
        // geohash cells. The flood fill must abort past MAX_COVERING_CELLS
        // and report an empty covering set (never a truncated/partial one),
        // so GeohashPruning's `geom.is_empty()` guard declines to prune
        // instead of enumerating (or worse, unsoundly truncating) millions
        // of cells at plan time.
        let huge = Rect::new(coord! { x: -180.0, y: -85.0 }, coord! { x: 180.0, y: 85.0 });
        let cells = geohash_covers_geometry(&Geometry::Rect(huge), 5);
        assert!(
            cells.is_empty(),
            "expected capped flood to report an empty covering set, got {} cells",
            cells.len()
        );

        // Sanity check the cap doesn't fire early: a modest bbox, well under
        // the cap, must still return its exact (non-empty) covering set.
        let modest = Rect::new(coord! { x: -2.0, y: -2.0 }, coord! { x: 2.0, y: 2.0 });
        let modest_cells = geohash_covers_geometry(&Geometry::Rect(modest), 4);
        assert!(
            !modest_cells.is_empty(),
            "modest bbox should still return a non-empty exact covering set"
        );
    }
}
