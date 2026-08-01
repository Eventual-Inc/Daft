//! `st_geohash_cells(geom, precision) -> List[Utf8]`: every geohash cell that
//! the geometry's BOUNDING BOX intersects.
//!
//! This is the covering used by the distributed grid spatial join: each build
//! row is exploded to one row per covering cell, so every pair whose bboxes
//! intersect meets in at least one common cell. Unlike the pruning-oriented
//! [`geohash_covers_geometry`](crate::st_geohash::geohash_covers_geometry) —
//! which returns EMPTY when the covering exceeds its cap (safe there: it just
//! disables pruning) — the strict variant here ERRORS on overflow: an empty
//! covering in a join explode would silently drop the row and lose true
//! matches.

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use geo::{BoundingRect, Centroid, Geometry};
use geohash::{Coord as GeohashCoord, encode};
use serde::{Deserialize, Serialize};

use crate::st_geohash::{MAX_COVERING_CELLS, collect_covering_cells};
use crate::utils::{get_geometry_binary, parse_wkb, read_f64_arg, validate_geometry_field};

/// Cells covering the geometry's bounding box. Errors when the covering
/// exceeds [`MAX_COVERING_CELLS`] — callers that explode rows by these cells
/// must fail loudly rather than silently losing the row.
pub fn geohash_covers_geometry_strict(g: &Geometry, precision: usize) -> DaftResult<Vec<String>> {
    let bbox = match g.bounding_rect() {
        Some(b) => b,
        None => {
            // Degenerate geometry without a bbox: fall back to its centroid's
            // cell (same behavior as the lenient covering).
            if let Some(c) = g.centroid() {
                let coord = GeohashCoord { x: c.x(), y: c.y() };
                return Ok(encode(coord, precision).ok().into_iter().collect());
            }
            return Ok(vec![]);
        }
    };

    let Ok(start_hash) = encode(
        GeohashCoord {
            x: bbox.min().x,
            y: bbox.min().y,
        },
        precision,
    ) else {
        // Coordinates outside the geohash domain can never be matched by a
        // cell-partitioned join anyway.
        return Ok(vec![]);
    };

    let mut cells = std::collections::HashSet::new();
    if collect_covering_cells(&mut cells, &start_hash, &bbox, precision) {
        return Err(DaftError::ValueError(format!(
            "st_geohash_cells: geometry bounding box covers more than {MAX_COVERING_CELLS} \
             geohash cells at precision {precision}; use a coarser precision for such large \
             geometries"
        )));
    }
    Ok(cells.into_iter().collect())
}

/// Adaptive covering: the FINEST precision in `[min_p, max_p]` whose covering
/// stays within `target_cells`, walking coarser as needed. Errors only when
/// even `min_p` exceeds the global cap — the escape hatch that lets one
/// dataset mix building-scale and region-scale geometries without either a
/// cap error or a uniform-coarse explosion.
pub fn geohash_cells_adaptive(
    g: &Geometry,
    min_p: usize,
    max_p: usize,
    target_cells: usize,
) -> DaftResult<Vec<String>> {
    for p in (min_p..=max_p).rev() {
        // The strict covering errors past the GLOBAL cap; treat that as
        // "too fine, keep coarsening" unless we're already at min_p.
        match geohash_covers_geometry_strict(g, p) {
            Ok(cells) if cells.len() <= target_cells => return Ok(cells),
            Ok(_) | Err(_) if p > min_p => continue,
            Ok(cells) => return Ok(cells), // min_p: accept even over target
            Err(e) => return Err(e),       // min_p and over the global cap
        }
    }
    unreachable!("loop always returns at min_p")
}

/// Ladder covering: the union of coverings at EVERY precision in
/// `[min_p, max_p]`. The probe side of an adaptive grid join emits this so it
/// can meet build rows at whatever precision each chose; for a point it is
/// exactly the geohash prefixes of its finest cell.
pub fn geohash_cells_ladder(g: &Geometry, min_p: usize, max_p: usize) -> DaftResult<Vec<String>> {
    let mut out = Vec::new();
    for p in min_p..=max_p {
        out.extend(geohash_covers_geometry_strict(g, p)?);
    }
    Ok(out)
}

/// Evaluate the covering per row of a Geometry/Binary series → List[Utf8].
/// Null or unparseable geometry rows produce NULL lists (which
/// `explode(ignore_empty_and_null)` drops — such rows can never match).
pub(crate) fn eval_geohash_cells(series: &Series, precision: usize) -> DaftResult<Series> {
    eval_cells_with(series, "st_geohash_cells", |g| geohash_covers_geometry_strict(g, precision))
}

/// Shared List[Utf8] series construction over a per-geometry covering fn.
fn eval_cells_with(
    series: &Series,
    out_name: &str,
    cells_of: impl Fn(&Geometry) -> DaftResult<Vec<String>>,
) -> DaftResult<Series> {
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use daft_core::prelude::ListArray;

    let binary = get_geometry_binary(series)?;

    let mut flat: Vec<String> = Vec::new();
    let mut offsets: Vec<i64> = Vec::with_capacity(binary.len() + 1);
    let mut validity: Vec<bool> = Vec::with_capacity(binary.len());
    offsets.push(0);
    for opt in binary.into_iter() {
        match opt.and_then(|b| parse_wkb(b).ok()) {
            Some(g) => {
                flat.extend(cells_of(&g)?);
                validity.push(true);
            }
            None => validity.push(false),
        }
        offsets.push(flat.len() as i64);
    }

    let child = Utf8Array::from_iter("cells", flat.iter().map(|s| Some(s.as_str()))).into_series();
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::from_iter(validity);
    Ok(ListArray::new(
        Field::new(out_name, DataType::List(Box::new(DataType::Utf8))),
        child,
        offsets,
        Some(nulls),
    )
    .into_series())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohashCells {
    pub precision: u8,
}

#[typetag::serde]
impl ScalarUDF for StGeohashCells {
    fn name(&self) -> &'static str {
        "st_geohash_cells"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        // Precision from the literal second argument (Python/SQL path), or
        // from self for the Rust-API path which registers it at build time.
        let precision = match inputs.optional(1)? {
            Some(_) => read_f64_arg(&inputs, 1, "precision", self.name())? as usize,
            None => self.precision as usize,
        };
        eval_geohash_cells(inputs.required(0)?, precision)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(
            self.name(),
            DataType::List(Box::new(DataType::Utf8)),
        ))
    }

    fn docstring(&self) -> &'static str {
        "Returns every geohash cell (at the given precision) that the geometry's bounding box intersects, as a list of strings. Errors if the covering exceeds the internal cell cap."
    }
}

#[must_use]
pub fn st_geohash_cells(geom: ExprRef, precision: u8) -> ExprRef {
    ScalarFn::builtin(StGeohashCells { precision }, vec![geom]).into()
}

/// Adaptive per-row covering (see [`geohash_cells_adaptive`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohashCellsAdaptive {
    pub min_precision: u8,
    pub max_precision: u8,
    pub target_cells: u16,
}

#[typetag::serde]
impl ScalarUDF for StGeohashCellsAdaptive {
    fn name(&self) -> &'static str {
        "st_geohash_cells_adaptive"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        eval_cells_with(inputs.required(0)?, self.name(), |g| {
            geohash_cells_adaptive(
                g,
                self.min_precision as usize,
                self.max_precision as usize,
                self.target_cells as usize,
            )
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::List(Box::new(DataType::Utf8))))
    }

    fn docstring(&self) -> &'static str {
        "Bounding-box geohash covering at a per-row adaptive precision: the finest precision in range whose covering fits the target cell count."
    }
}

#[must_use]
pub fn st_geohash_cells_adaptive(
    geom: ExprRef,
    min_precision: u8,
    max_precision: u8,
    target_cells: u16,
) -> ExprRef {
    ScalarFn::builtin(
        StGeohashCellsAdaptive {
            min_precision,
            max_precision,
            target_cells,
        },
        vec![geom],
    )
    .into()
}

/// Multi-precision ladder covering (see [`geohash_cells_ladder`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StGeohashCellsLadder {
    pub min_precision: u8,
    pub max_precision: u8,
}

#[typetag::serde]
impl ScalarUDF for StGeohashCellsLadder {
    fn name(&self) -> &'static str {
        "st_geohash_cells_ladder"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        eval_cells_with(inputs.required(0)?, self.name(), |g| {
            geohash_cells_ladder(g, self.min_precision as usize, self.max_precision as usize)
        })
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::List(Box::new(DataType::Utf8))))
    }

    fn docstring(&self) -> &'static str {
        "Union of bounding-box geohash coverings at every precision in range — for a point, the geohash prefixes of its finest cell."
    }
}

#[must_use]
pub fn st_geohash_cells_ladder(geom: ExprRef, min_precision: u8, max_precision: u8) -> ExprRef {
    ScalarFn::builtin(
        StGeohashCellsLadder {
            min_precision,
            max_precision,
        },
        vec![geom],
    )
    .into()
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
    use geo::{Coord, Geometry, LineString, Point, Polygon};

    use super::*;
    use crate::st_geohash::geohash_covers_geometry;
    use crate::utils::geom_to_wkb;

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Geometry {
        let ring = LineString(vec![
            Coord { x: x0, y: y0 },
            Coord { x: x1, y: y0 },
            Coord { x: x1, y: y1 },
            Coord { x: x0, y: y1 },
            Coord { x: x0, y: y0 },
        ]);
        Geometry::Polygon(Polygon::new(ring, vec![]))
    }

    /// A point covers exactly its own cell.
    #[test]
    fn point_covers_exactly_one_cell() {
        let g = Geometry::Point(Point::new(151.2, -33.9));
        let cells = geohash_covers_geometry_strict(&g, 6).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells, geohash_covers_geometry(&g, 6));
    }

    /// Small non-capped coverings must agree with the lenient covering
    /// function used by the pruning rule.
    #[test]
    fn small_rect_matches_lenient_covering() {
        let g = rect(151.20, -33.90, 151.23, -33.88); // spans several gh6 cells
        let mut strict = geohash_covers_geometry_strict(&g, 6).unwrap();
        let mut lenient = geohash_covers_geometry(&g, 6);
        strict.sort();
        lenient.sort();
        assert!(strict.len() > 1, "rect should span multiple cells");
        assert_eq!(strict, lenient);
    }

    /// The lenient function returns EMPTY when the covering exceeds its cap —
    /// safe for pruning (just disables it) but UNSOUND for a join explode
    /// (the row would silently vanish and drop true matches). The strict
    /// variant must ERROR instead.
    #[test]
    fn oversized_covering_errors_instead_of_empty() {
        let g = rect(-170.0, -80.0, 170.0, 80.0); // the whole world at gh6
        assert!(geohash_covers_geometry(&g, 6).is_empty(), "lenient caps to empty");
        let err = geohash_covers_geometry_strict(&g, 6).unwrap_err();
        assert!(
            err.to_string().contains("cells"),
            "error should mention the cell cap: {err}"
        );
    }

    /// Adaptive covering: small geometries get the finest precision, big ones
    /// walk coarser until the per-row target is met — never a cap error for
    /// anything under continental scale.
    #[test]
    fn adaptive_picks_precision_by_size() {
        // ~10 m box → finest precision (6), same cells as the strict covering.
        let small = rect(151.2000, -33.9000, 151.2001, -33.9001);
        let cells = geohash_cells_adaptive(&small, 4, 6, 64).unwrap();
        assert!(!cells.is_empty());
        assert!(cells.iter().all(|c| c.len() == 6), "small geom stays at gh6: {cells:?}");

        // ~0.18° (~17 km) box → over 64 cells at gh6, fits at gh5.
        let monster = rect(151.0, -34.0, 151.18, -33.82);
        assert!(geohash_covers_geometry_strict(&monster, 6).unwrap().len() > 64);
        let cells = geohash_cells_adaptive(&monster, 4, 6, 64).unwrap();
        assert!(cells.iter().all(|c| c.len() == 5), "17km geom coarsens to gh5: got lengths {:?}", cells.iter().map(|c| c.len()).collect::<Vec<_>>());
        assert!(cells.len() <= 64);

        // ~2° (~180 km) box → would ERROR at fixed gh6 (66k cells > cap),
        // adaptive lands at gh4.
        let huge = rect(150.0, -34.0, 152.0, -32.0);
        assert!(geohash_covers_geometry_strict(&huge, 6).is_err());
        let cells = geohash_cells_adaptive(&huge, 4, 6, 64).unwrap();
        assert!(cells.iter().all(|c| c.len() == 4));
        // At min_p the covering is accepted even over target (documented) —
        // only the GLOBAL cap can still error.
        assert!(!cells.is_empty());
    }

    /// The ladder covering is the union of coverings at every precision in
    /// range — for a point that is exactly its cell's geohash PREFIXES.
    #[test]
    fn ladder_emits_prefixes_for_points() {
        let p = Geometry::Point(Point::new(151.2, -33.9));
        let mut cells = geohash_cells_ladder(&p, 4, 6).unwrap();
        cells.sort_by_key(|c| c.len());
        assert_eq!(cells.len(), 3);
        let fine = cells[2].clone();
        assert_eq!(cells[0], fine[..4].to_string());
        assert_eq!(cells[1], fine[..5].to_string());
        // Cross-check against per-precision strict coverings.
        for (i, prec) in (4..=6).enumerate() {
            assert_eq!(vec![cells[i].clone()], geohash_covers_geometry_strict(&p, prec).unwrap());
        }
    }

    /// Series-level UDF: List[Utf8] output, one list per row, null geometry
    /// rows produce null lists (which `explode(ignore_empty_and_null)` drops).
    #[test]
    fn udf_produces_list_of_cells_with_null_passthrough() {
        let wkbs: Vec<Option<Vec<u8>>> = vec![
            Some(geom_to_wkb(&Geometry::Point(Point::new(151.2, -33.9))).unwrap()),
            None,
            Some(geom_to_wkb(&rect(151.20, -33.90, 151.23, -33.88)).unwrap()),
        ];
        let series =
            BinaryArray::from_iter("geom", wkbs.iter().map(|o| o.as_deref())).into_series();

        let out = eval_geohash_cells(&series, 6).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(
            out.data_type(),
            &DataType::List(Box::new(DataType::Utf8))
        );
        let list = out.list().unwrap();
        let row0 = list.get(0).expect("valid row");
        assert_eq!(row0.len(), 1);
        assert!(list.get(1).is_none(), "null geom row must be a null list");
        let row2 = list.get(2).expect("valid row");
        assert!(row2.len() > 1);
    }
}
