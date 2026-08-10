//! Bbox-conjunct rewrite for pure spatial WHERE clauses.
//!
//! When a filter applies a spatial predicate between a geometry column and a
//! LITERAL query geometry, and the schema carries precomputed bbox Float64
//! columns (canonical `rtree_*` names, as written by `df.with_spatial_bbox()`
//! and the `*_geo` ETL layout; `min_*`/`bbox_*` accepted for compatibility),
//! rewrite the filter to AND in plain float comparisons against the query
//! geometry's MBR:
//!
//! ```sql
//! -- Original
//! WHERE st_intersects(geom, @query)
//! -- Rewritten
//! WHERE rtree_max_x >= @qminx AND rtree_min_x <= @qmaxx
//!   AND rtree_max_y >= @qminy AND rtree_min_y <= @qmaxy
//!   AND st_intersects(geom, @query)
//! ```
//!
//! The added conjuncts are ordinary column/literal comparisons, so
//! `PushDownFilter` pushes them into the scan and the existing parquet
//! row-group/file min-max statistics prune data with NO sidecar index and NO
//! geohash column — zero preparation beyond the bbox columns themselves.
//!
//! Soundness: a conjunct is only added when the spatial predicate being TRUE
//! implies it (superset property), per predicate and argument order:
//! - `st_intersects` (either order) / `st_dwithin` (query MBR padded by its
//!   literal distance): the bboxes must intersect.
//! - `st_contains(col, Q)` / `st_within(Q, col)`: col's bbox must CONTAIN
//!   Q's bbox (tighter than intersection).
//! - `st_contains(Q, col)` / `st_within(col, Q)`: col's bbox must lie WITHIN
//!   Q's bbox.
//! - `st_disjoint`, negations, and OR-compositions are never rewritten.
//!
//! Trust contract: like the spatial join's bbox fast path, the bbox columns
//! are trusted to describe the geometry column's MBR; rows with NULL bbox
//! values are treated as non-matching (the comparisons evaluate to null).

use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_algebra::boolean::{combine_conjunction, split_conjunction};
use daft_core::prelude::Operator;
use daft_dsl::{
    Expr, ExprRef,
    expr::{Column, ResolvedColumn},
    lit, resolved_col,
};
use daft_geo::wkb_to_mbr;

use super::OptimizerRule;
use crate::LogicalPlan;

const CANDIDATE_BBOX_SETS: [(&str, &str, &str, &str); 3] = [
    ("rtree_min_x", "rtree_min_y", "rtree_max_x", "rtree_max_y"),
    ("min_x", "min_y", "max_x", "max_y"),
    ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y"),
];

/// Optimizer rule: add sound, pushdown-friendly bbox comparisons alongside
/// literal-query spatial predicates. Registered `Once`, before PushDownFilter.
#[derive(Debug, Default)]
pub struct SpatialBboxPruning;

impl OptimizerRule for SpatialBboxPruning {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        plan.transform(|node| self.try_optimize_node(node))
    }
}

impl SpatialBboxPruning {
    fn try_optimize_node(
        &self,
        plan: Arc<LogicalPlan>,
    ) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let filter = match plan.as_ref() {
            LogicalPlan::Filter(f) => f,
            _ => return Ok(Transformed::no(plan)),
        };

        let schema = filter.input.schema();
        let Some(bbox_cols) = find_bbox_cols(&schema) else {
            return Ok(Transformed::no(plan));
        };

        // The rtree_* columns describe ONE geometry column's bbox, but the
        // canonical names carry no provenance. With more than one geometry
        // column in scope (e.g. post-join schemas), a predicate on the OTHER
        // geometry would be pruned against the wrong bbox and silently drop
        // true matches — refuse the rewrite entirely in that case.
        let n_geom_cols = schema
            .into_iter()
            .filter(|f| {
                matches!(
                    f.dtype,
                    daft_schema::dtype::DataType::Geometry | daft_schema::dtype::DataType::Binary
                )
            })
            .count();
        if n_geom_cols > 1 {
            return Ok(Transformed::no(plan));
        }

        // Only TOP-LEVEL conjuncts are sound to augment: under OR / NOT the
        // spatial predicate's truth doesn't constrain the row's bbox.
        let conjuncts = split_conjunction(&filter.predicate);
        let mut extra: Vec<ExprRef> = vec![];
        for c in &conjuncts {
            if let Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) = c.as_ref()
            {
                if let Some(pred) = bbox_preds_for_spatial_fn(sf, &bbox_cols) {
                    // Idempotency: skip conjuncts that are already present
                    // (e.g. a second optimizer application on rewritten output).
                    extra.extend(
                        pred.into_iter().filter(|p| !conjuncts.contains(p)),
                    );
                }
            }
        }

        if extra.is_empty() {
            return Ok(Transformed::no(plan));
        }

        extra.push(filter.predicate.clone());
        let new_predicate = combine_conjunction(extra).unwrap();
        let new_filter = LogicalPlan::from(crate::ops::Filter::try_new(
            filter.input.clone(),
            new_predicate,
        )?)
        .into();
        Ok(Transformed::yes(new_filter))
    }
}

/// Locate a full Float64 bbox column set by canonical names. All four must be
/// Float64 — a name collision at another dtype disables the rewrite.
fn find_bbox_cols(schema: &daft_schema::schema::Schema) -> Option<[String; 4]> {
    let is_f64 = |name: &str| {
        schema
            .get_field(name)
            .ok()
            .is_some_and(|f| f.dtype == daft_schema::dtype::DataType::Float64)
    };
    CANDIDATE_BBOX_SETS.iter().find_map(|(mn_x, mn_y, mx_x, mx_y)| {
        (is_f64(mn_x) && is_f64(mn_y) && is_f64(mx_x) && is_f64(mx_y)).then(|| {
            [
                (*mn_x).to_string(),
                (*mn_y).to_string(),
                (*mx_x).to_string(),
                (*mx_y).to_string(),
            ]
        })
    })
}

/// How the geometry COLUMN's bbox is constrained by the predicate being true.
enum BboxForm {
    /// Bboxes must intersect.
    Intersect,
    /// The column's bbox must contain the query's bbox.
    ColContainsQuery,
    /// The column's bbox must lie within the query's bbox.
    ColWithinQuery,
}

/// Return the bbox comparison conjuncts implied by this spatial call, or
/// `None` when no sound rewrite exists (unknown fn, no literal side, no
/// column side, unparseable geometry, unknown dwithin distance).
fn bbox_preds_for_spatial_fn(
    sf: &daft_dsl::functions::BuiltinScalarFn,
    bbox_cols: &[String; 4],
) -> Option<Vec<ExprRef>> {
    let name = sf.name();
    let arg0 = sf.inputs.required(0).ok()?;
    let arg1 = sf.inputs.required(1).ok()?;

    // Identify which side is the geometry column and which the WKB literal.
    let col_is_arg0 = match (is_geom_col(arg0), wkb_literal(arg1)) {
        (true, Some(_)) => true,
        _ => match (wkb_literal(arg0), is_geom_col(arg1)) {
            (Some(_), true) => false,
            _ => return None,
        },
    };
    let wkb = if col_is_arg0 {
        wkb_literal(arg1)?
    } else {
        wkb_literal(arg0)?
    };

    let form = match (name, col_is_arg0) {
        ("st_intersects", _) | ("st_dwithin", _) => BboxForm::Intersect,
        // covers/contains: the container's bbox contains the containee's.
        ("st_contains" | "st_covers", true) | ("st_within" | "st_covered_by", false) => {
            BboxForm::ColContainsQuery
        }
        ("st_contains" | "st_covers", false) | ("st_within" | "st_covered_by", true) => {
            BboxForm::ColWithinQuery
        }
        _ => return None,
    };

    // st_dwithin: pad the query MBR by the literal distance. A non-literal,
    // negative, or non-finite distance means the pad is unknown — refuse the
    // rewrite entirely (never under-pad).
    let pad = if name == "st_dwithin" {
        let d = sf.inputs.required(2).ok()?;
        let l = d.as_literal()?;
        let val = l.as_f64().or_else(|| l.as_i64().map(|v| v as f64))?;
        if !val.is_finite() || val < 0.0 {
            return None;
        }
        val
    } else {
        0.0
    };

    let [q_min_x, q_min_y, q_max_x, q_max_y] = wkb_to_mbr(wkb)?;
    let (q_min_x, q_min_y, q_max_x, q_max_y) =
        (q_min_x - pad, q_min_y - pad, q_max_x + pad, q_max_y + pad);
    if ![q_min_x, q_min_y, q_max_x, q_max_y]
        .iter()
        .all(|v| v.is_finite())
    {
        return None;
    }

    let [mn_x, mn_y, mx_x, mx_y] = bbox_cols;
    let c = |n: &String| resolved_col(n.as_str());
    Some(match form {
        BboxForm::Intersect => vec![
            c(mx_x).gt_eq(lit(q_min_x)),
            c(mn_x).lt_eq(lit(q_max_x)),
            c(mx_y).gt_eq(lit(q_min_y)),
            c(mn_y).lt_eq(lit(q_max_y)),
        ],
        BboxForm::ColContainsQuery => vec![
            c(mn_x).lt_eq(lit(q_min_x)),
            c(mx_x).gt_eq(lit(q_max_x)),
            c(mn_y).lt_eq(lit(q_min_y)),
            c(mx_y).gt_eq(lit(q_max_y)),
        ],
        BboxForm::ColWithinQuery => vec![
            c(mn_x).gt_eq(lit(q_min_x)),
            c(mx_x).lt_eq(lit(q_max_x)),
            c(mn_y).gt_eq(lit(q_min_y)),
            c(mx_y).lt_eq(lit(q_max_y)),
        ],
    })
}

fn is_geom_col(e: &ExprRef) -> bool {
    match e.as_ref() {
        Expr::Alias(inner, _) => is_geom_col(inner),
        Expr::Column(Column::Resolved(ResolvedColumn::Basic(_))) => true,
        _ => false,
    }
}

fn wkb_literal(e: &ExprRef) -> Option<&[u8]> {
    match e.as_ref() {
        Expr::Literal(daft_core::lit::Literal::Binary(b)) => Some(b.as_slice()),
        Expr::Alias(inner, _) => wkb_literal(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::Operator;
    use daft_dsl::{Expr, ExprRef, resolved_col};
    use daft_geo::geo::{Coord, Geometry, LineString, Point, Polygon};
    use daft_geo::utils::geom_to_wkb;
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;
    use crate::{
        ClusteringSpec, LogicalPlan, LogicalPlanRef, SourceInfo, logical_plan::Source,
        source_info::PlaceHolderInfo,
    };

    fn source_with(fields: Vec<Field>) -> LogicalPlanRef {
        let schema = Arc::new(Schema::new(fields));
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown(0)),
            })),
        ))
        .arced()
    }

    fn geo_source() -> LogicalPlanRef {
        source_with(vec![
            Field::new("geom", DataType::Binary),
            Field::new("rtree_min_x", DataType::Float64),
            Field::new("rtree_min_y", DataType::Float64),
            Field::new("rtree_max_x", DataType::Float64),
            Field::new("rtree_max_y", DataType::Float64),
        ])
    }

    /// Query polygon with bbox (10, 20) - (30, 40).
    fn query_wkb_lit() -> ExprRef {
        let ring = LineString(vec![
            Coord { x: 10.0, y: 20.0 },
            Coord { x: 30.0, y: 20.0 },
            Coord { x: 30.0, y: 40.0 },
            Coord { x: 10.0, y: 40.0 },
            Coord { x: 10.0, y: 20.0 },
        ]);
        let wkb = geom_to_wkb(&Geometry::Polygon(Polygon::new(ring, vec![]))).unwrap();
        Expr::Literal(daft_core::lit::Literal::Binary(wkb)).arced()
    }

    fn point_wkb_lit(x: f64, y: f64) -> ExprRef {
        let wkb = geom_to_wkb(&Geometry::Point(Point::new(x, y))).unwrap();
        Expr::Literal(daft_core::lit::Literal::Binary(wkb)).arced()
    }

    fn apply(input: LogicalPlanRef, predicate: ExprRef) -> (bool, ExprRef) {
        let plan: LogicalPlanRef =
            LogicalPlan::from(crate::ops::Filter::try_new(input, predicate).unwrap()).into();
        let result = SpatialBboxPruning.try_optimize(plan).unwrap();
        let transformed = result.transformed;
        let LogicalPlan::Filter(f) = result.data.as_ref() else {
            panic!("expected Filter after rule");
        };
        (transformed, f.predicate.clone())
    }

    /// True if some top-level conjunct is `col(name) <op> lit(value)`.
    fn has_cmp(pred: &ExprRef, name: &str, op: Operator, value: f64) -> bool {
        daft_algebra::boolean::split_conjunction(pred)
            .iter()
            .any(|c| match c.as_ref() {
                Expr::BinaryOp {
                    op: o,
                    left,
                    right,
                } if *o == op => {
                    let col_ok = matches!(
                        left.as_ref(),
                        Expr::Column(daft_dsl::expr::Column::Resolved(
                            daft_dsl::expr::ResolvedColumn::Basic(n)
                        )) if n.as_ref() == name
                    );
                    let lit_ok = matches!(
                        right.as_ref(),
                        Expr::Literal(daft_core::lit::Literal::Float64(v)) if *v == value
                    );
                    col_ok && lit_ok
                }
                _ => false,
            })
    }

    fn n_conjuncts(pred: &ExprRef) -> usize {
        daft_algebra::boolean::split_conjunction(pred).len()
    }

    /// st_intersects(col, Q) — true implies the bboxes intersect, so the
    /// intersect-form conjuncts must be added and the original kept.
    #[test]
    fn intersects_adds_intersect_form_conjuncts() {
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_max_x", Operator::GtEq, 10.0));
        assert!(has_cmp(&pred, "rtree_min_x", Operator::LtEq, 30.0));
        assert!(has_cmp(&pred, "rtree_max_y", Operator::GtEq, 20.0));
        assert!(has_cmp(&pred, "rtree_min_y", Operator::LtEq, 40.0));
        assert_eq!(n_conjuncts(&pred), 5, "4 bbox conjuncts + the original predicate");
    }

    /// st_contains(col, Q): the column's geometry must contain Q, so its bbox
    /// must CONTAIN Q's bbox — a tighter (more selective) form.
    #[test]
    fn contains_col_query_adds_containment_form() {
        let spatial = daft_geo::st_contains::st_contains(resolved_col("geom"), query_wkb_lit());
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_min_x", Operator::LtEq, 10.0));
        assert!(has_cmp(&pred, "rtree_max_x", Operator::GtEq, 30.0));
        assert!(has_cmp(&pred, "rtree_min_y", Operator::LtEq, 20.0));
        assert!(has_cmp(&pred, "rtree_max_y", Operator::GtEq, 40.0));
    }

    /// st_contains(Q, col): the column's geometry lies WITHIN Q, so its bbox
    /// must be inside Q's bbox.
    #[test]
    fn contains_query_col_adds_within_form() {
        let spatial = daft_geo::st_contains::st_contains(query_wkb_lit(), resolved_col("geom"));
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_min_x", Operator::GtEq, 10.0));
        assert!(has_cmp(&pred, "rtree_max_x", Operator::LtEq, 30.0));
        assert!(has_cmp(&pred, "rtree_min_y", Operator::GtEq, 20.0));
        assert!(has_cmp(&pred, "rtree_max_y", Operator::LtEq, 40.0));
    }

    /// st_covers has the same bbox implication as st_contains (boundary
    /// inclusion doesn't change the bbox containment).
    #[test]
    fn covers_col_query_adds_containment_form() {
        let spatial = daft_geo::st_covers::st_covers(resolved_col("geom"), query_wkb_lit());
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_min_x", Operator::LtEq, 10.0));
        assert!(has_cmp(&pred, "rtree_max_x", Operator::GtEq, 30.0));
    }

    /// st_within(col, Q) is the mirror of st_contains(Q, col).
    #[test]
    fn within_col_query_adds_within_form() {
        let spatial = daft_geo::st_within::st_within(resolved_col("geom"), query_wkb_lit());
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_min_x", Operator::GtEq, 10.0));
        assert!(has_cmp(&pred, "rtree_max_x", Operator::LtEq, 30.0));
    }

    /// st_dwithin pads the query bbox by the literal distance on all sides.
    #[test]
    fn dwithin_pads_query_bbox_by_distance() {
        let spatial = daft_geo::st_dwithin::st_dwithin(
            resolved_col("geom"),
            point_wkb_lit(100.0, 200.0),
            daft_dsl::lit(5.0),
        );
        let (transformed, pred) = apply(geo_source(), spatial);
        assert!(transformed);
        assert!(has_cmp(&pred, "rtree_max_x", Operator::GtEq, 95.0));
        assert!(has_cmp(&pred, "rtree_min_x", Operator::LtEq, 105.0));
        assert!(has_cmp(&pred, "rtree_max_y", Operator::GtEq, 195.0));
        assert!(has_cmp(&pred, "rtree_min_y", Operator::LtEq, 205.0));
    }

    /// A negative or non-finite distance means the required pad is unknown —
    /// the rewrite must be refused entirely (an under-padded bbox filter
    /// would silently drop matching rows). A non-numeric-literal distance is
    /// unrepresentable: Filter construction itself rejects it.
    #[test]
    fn dwithin_unknown_distance_no_rewrite() {
        for bad_distance in [daft_dsl::lit(-1.0), daft_dsl::lit(f64::NAN)] {
            let spatial = daft_geo::st_dwithin::st_dwithin(
                resolved_col("geom"),
                point_wkb_lit(0.0, 0.0),
                bad_distance,
            );
            let (transformed, _) = apply(geo_source(), spatial);
            assert!(!transformed);
        }
    }

    /// No bbox columns in the schema → no rewrite.
    #[test]
    fn missing_bbox_columns_no_rewrite() {
        let src = source_with(vec![Field::new("geom", DataType::Binary)]);
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let (transformed, _) = apply(src, spatial);
        assert!(!transformed);
    }

    /// Name collision with wrong dtype → no rewrite (comparing strings against
    /// float literals would be a type error, or worse, silently wrong).
    #[test]
    fn wrong_dtype_bbox_columns_no_rewrite() {
        let src = source_with(vec![
            Field::new("geom", DataType::Binary),
            Field::new("rtree_min_x", DataType::Utf8),
            Field::new("rtree_min_y", DataType::Float64),
            Field::new("rtree_max_x", DataType::Float64),
            Field::new("rtree_max_y", DataType::Float64),
        ]);
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let (transformed, _) = apply(src, spatial);
        assert!(!transformed);
    }

    /// OR-composed spatial predicates must NOT be rewritten: the other branch
    /// can be true for rows whose bboxes do not intersect the query.
    #[test]
    fn or_composed_spatial_no_rewrite() {
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let pred = spatial.or(resolved_col("geom").is_null());
        let (transformed, _) = apply(geo_source(), pred);
        assert!(!transformed);
    }

    /// st_disjoint is true precisely when bboxes may NOT intersect — never prunable.
    #[test]
    fn disjoint_no_rewrite() {
        let spatial = daft_geo::st_disjoint::st_disjoint(resolved_col("geom"), query_wkb_lit());
        let (transformed, _) = apply(geo_source(), spatial);
        assert!(!transformed);
    }

    /// AND-composed spatial conjunct is rewritten, other conjuncts preserved.
    #[test]
    fn and_composed_spatial_is_rewritten() {
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let pred = spatial.and(resolved_col("geom").not_null());
        let (transformed, new_pred) = apply(geo_source(), pred);
        assert!(transformed);
        assert!(has_cmp(&new_pred, "rtree_max_x", Operator::GtEq, 10.0));
        assert_eq!(n_conjuncts(&new_pred), 6, "4 bbox + spatial + not_null");
    }

    /// Applying the rule to its own output must be a no-op — the added
    /// conjuncts are recognized and not duplicated.
    #[test]
    fn rule_is_idempotent() {
        let spatial = daft_geo::st_intersects::st_intersects(resolved_col("geom"), query_wkb_lit());
        let (transformed, rewritten) = apply(geo_source(), spatial);
        assert!(transformed);
        let (transformed_again, again) = apply(geo_source(), rewritten.clone());
        assert!(!transformed_again, "second application must not change the plan");
        assert_eq!(n_conjuncts(&again), n_conjuncts(&rewritten));
    }
}
