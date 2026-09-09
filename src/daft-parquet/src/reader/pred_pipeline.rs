//! LM-pipelined (Abadi et al. 2007, the "late materialization / pipelined
//! predicate evaluation" line of work) conjunctive predicate planning for the
//! Parquet reader.
//!
//! A pushed-down conjunction like `a > 5 AND b < 10 AND c = 'foo'` is split into
//! per-column *groups*. The reader can then evaluate one group at a time,
//! progressively narrowing the [`parquet::arrow::arrow_reader::RowSelection`] so
//! each later group decodes only the rows that survived every earlier group —
//! instead of decoding all predicate columns up-front and evaluating one
//! monolithic mask.
//!
//! # Correctness
//!
//! This module only decides **split / group / order**. For a conjunction the
//! surviving-row set is order-independent: a row survives iff *every* conjunct
//! is true, and progressive narrowing only ever drops rows that already failed
//! an earlier conjunct (which would fail the AND anyway). Reordering therefore
//! changes **how many rows later groups decode**, never **which rows survive**.
//! Any estimation failure degrades to a neutral ordering, not to a wrong result.

use std::{
    collections::{HashMap, HashSet},
    ops::Index,
    sync::OnceLock,
};

use daft_core::{
    lit::Literal,
    prelude::{DataType, Operator, Schema},
};
use daft_dsl::{
    Expr, ExprRef,
    common_treenode::{TreeNode, TreeNodeRecursion},
    optimization::get_required_columns,
};
use daft_stats::{ColumnRangeStatistics, TableStatistics};
use parquet::file::metadata::RowGroupMetaData;

use crate::statistics::row_group_metadata_to_table_stats;

/// Kill-switch for the pipelined path. Default on; set `DAFT_PARQUET_LM_PIPELINE=0`
/// to force the monolithic prefilter (useful for A/B comparison and safe rollout).
pub(super) fn lm_pipeline_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("DAFT_PARQUET_LM_PIPELINE")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

/// Kill-switch for stats-based *string* selectivity in group ordering. Default
/// on; set `DAFT_PARQUET_LM_STR_STATS=0` to fall back to the constant estimate
/// for `Utf8` range/equality conjuncts. Lets the ordering improvement be A/B'd
/// in isolation (both modes still take the pipelined path) and gives a safe
/// rollout valve. Ordering never affects the result set, so this is purely a
/// performance knob.
fn lm_str_stats_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("DAFT_PARQUET_LM_STR_STATS")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

/// Kill-switch for the metadata-driven *cost* axis of the ordering rank. Default
/// on: a group's decode cost is the summed uncompressed byte size of its column
/// chunks in the row group (from [`RowGroupMetaData`]). Set
/// `DAFT_PARQUET_LM_COST_BYTES=0` to fall back to the crude "number of columns"
/// proxy. Like the other knobs this only affects ordering, hence performance.
fn lm_cost_bytes_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("DAFT_PARQUET_LM_COST_BYTES")
            .map(|v| v != "0")
            .unwrap_or(true)
    })
}

/// Split an AND-chain into its top-level conjuncts, skipping `Alias` wrappers and
/// descending through nested `And` nodes. Mirrors `daft_algebra::boolean::
/// split_conjunction`; inlined here so `daft-parquet` needn't take a new crate
/// dependency for a ~10-line helper (the tree-node imports already exist in this
/// crate).
pub(super) fn split_conjunction(expr: &ExprRef) -> Vec<ExprRef> {
    let mut splits = Vec::new();
    let _ = expr.apply(|e| match e.as_ref() {
        Expr::BinaryOp {
            op: Operator::And, ..
        }
        | Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
        _ => {
            splits.push(e.clone());
            Ok(TreeNodeRecursion::Jump)
        }
    });
    splits
}

/// Re-AND a set of expressions. `None` for an empty iterator; a single expression
/// is returned as-is. Mirrors `daft_algebra::boolean::combine_conjunction`.
fn combine_conjunction(exprs: Vec<ExprRef>) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, e| acc.and(e))
}

/// A column-disjoint unit of predicate evaluation. All conjuncts that (transitively)
/// share a column are merged into one group so each physical column is decoded
/// exactly once; the group's `subpred` is the AND of its conjuncts.
#[derive(Clone, Debug)]
pub(super) struct PredGroup {
    /// AND of every conjunct assigned to this group.
    pub(super) subpred: ExprRef,
    /// Distinct column names referenced by `subpred` (sorted, deduped).
    pub(super) col_names: Vec<String>,
}

/// Merge conjuncts into column-disjoint groups.
///
/// Two conjuncts land in the same group if their required-column sets intersect,
/// merged transitively (so `a>5`, `b<10`, `a<b` collapse into a single group —
/// they cannot be narrowed independently). Returns groups in first-appearance
/// order; ordering by selectivity happens later in [`order_groups`].
pub(super) fn group_conjuncts(conjuncts: Vec<ExprRef>) -> Vec<PredGroup> {
    // (accumulated column set, conjuncts) per group.
    let mut groups: Vec<(HashSet<String>, Vec<ExprRef>)> = Vec::new();
    for c in conjuncts {
        let cols: HashSet<String> = get_required_columns(&c).into_iter().collect();
        match groups
            .iter()
            .position(|(gc, _)| gc.iter().any(|n| cols.contains(n)))
        {
            Some(pos) => {
                groups[pos].0.extend(cols);
                groups[pos].1.push(c);
            }
            None => groups.push((cols, vec![c])),
        }
    }

    // Transitive closure: a later conjunct may bridge two earlier groups.
    let mut changed = true;
    while changed {
        changed = false;
        'outer: for i in 0..groups.len() {
            for j in (i + 1)..groups.len() {
                if groups[i].0.iter().any(|n| groups[j].0.contains(n)) {
                    let (jc, jconj) = groups.remove(j);
                    groups[i].0.extend(jc);
                    groups[i].1.extend(jconj);
                    changed = true;
                    break 'outer;
                }
            }
        }
    }

    groups
        .into_iter()
        .filter_map(|(cols, conj)| {
            let subpred = combine_conjunction(conj)?;
            let mut col_names: Vec<String> = cols.into_iter().collect();
            col_names.sort();
            col_names.dedup();
            Some(PredGroup { subpred, col_names })
        })
        .collect()
}

/// Plan the pipelined groups for `predicate`, or `None` if pipelining does not
/// apply (fewer than two column-disjoint groups → the monolithic prefilter is
/// already optimal, e.g. a single-column or fully column-overlapping predicate).
pub(super) fn plan_pred_groups(predicate: &ExprRef) -> Option<Vec<PredGroup>> {
    let conjuncts = split_conjunction(predicate);
    if conjuncts.len() < 2 {
        return None;
    }
    let groups = group_conjuncts(conjuncts);
    (groups.len() >= 2).then_some(groups)
}

/// Per-row-group estimation inputs for ordering. Bundling them keeps the
/// selectivity/cost helpers read-only and cheap to thread through the sort.
struct SelCtx<'a> {
    /// Column min/max ranges (Daft's [`TableStatistics`]), if convertible.
    stats: Option<&'a TableStatistics>,
    schema: &'a Schema,
    /// top-level column name → distinct_count from Parquet stats (when written).
    ndv: HashMap<String, usize>,
    /// top-level column name → summed uncompressed chunk bytes in this row group.
    cost: HashMap<String, f64>,
    /// Row count of this row group (denominator context for NDV selectivity).
    #[allow(dead_code)]
    rg_rows: i64,
}

/// Order `groups` for a specific row group by an estimated rank, cheapest-and-
/// most-selective first. Returns group indices into `groups` (a permutation).
///
/// Rank is the classic predicate-ordering heuristic `(selectivity - 1) / cost`,
/// i.e. benefit `(1 - selectivity) / cost` sorted descending. Both axes come from
/// Parquet metadata: selectivity from column stats (min/max, distinct_count) and
/// cost from column-chunk byte sizes. More negative = narrow more rows per unit
/// of decode work = go first. Ties keep the caller's original order (stable sort).
pub(super) fn order_groups(
    groups: &[PredGroup],
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> Vec<usize> {
    let stats = row_group_metadata_to_table_stats(rg_meta, schema).ok();
    let ctx = SelCtx {
        stats: stats.as_ref(),
        schema,
        ndv: column_ndv(rg_meta),
        cost: column_costs(rg_meta),
        rg_rows: rg_meta.num_rows(),
    };

    let mut order: Vec<usize> = (0..groups.len()).collect();
    order.sort_by(|&a, &b| {
        let ra = group_rank(&groups[a], &ctx);
        let rb = group_rank(&groups[b], &ctx);
        ra.partial_cmp(&rb).unwrap_or(std::cmp::Ordering::Equal)
    });
    order
}

/// top-level column name → distinct_count, for columns whose Parquet stats carry
/// it. Many writers omit distinct_count, so the map is often partial/empty and
/// equality selectivity then falls back to a constant.
fn column_ndv(rg_meta: &RowGroupMetaData) -> HashMap<String, usize> {
    let mut ndv: HashMap<String, usize> = HashMap::new();
    for col in rg_meta.columns() {
        let Some(name) = top_level_name(col) else {
            continue;
        };
        if let Some(d) = col.statistics().and_then(|s| s.distinct_count_opt()) {
            let d = d as usize;
            // A top-level column may span several physical chunks; keep the widest
            // NDV (the least-selective, safest estimate for ordering).
            ndv.entry(name)
                .and_modify(|v| *v = (*v).max(d))
                .or_insert(d);
        }
    }
    ndv
}

/// top-level column name → summed uncompressed chunk bytes in this row group.
/// Uncompressed size tracks the value/offset materialization work that late
/// materialization saves (decompression is cheap relative to it).
fn column_costs(rg_meta: &RowGroupMetaData) -> HashMap<String, f64> {
    let mut cost = HashMap::new();
    for col in rg_meta.columns() {
        let Some(name) = top_level_name(col) else {
            continue;
        };
        *cost.entry(name).or_insert(0.0) += col.uncompressed_size().max(0) as f64;
    }
    cost
}

fn top_level_name(col: &parquet::file::metadata::ColumnChunkMetaData) -> Option<String> {
    col.column_descr()
        .path()
        .parts()
        .first()
        .map(|s| s.as_str().to_string())
}

fn group_rank(group: &PredGroup, ctx: &SelCtx) -> f64 {
    let sel = group_selectivity(group, ctx);
    let cost = group_cost(group, ctx);
    (sel - 1.0) / cost
}

/// Decode cost of a group: summed uncompressed bytes of its columns when the
/// metadata-driven cost axis is on, else the crude column-count proxy. Clamped
/// `>= 1` so the rank never divides by zero.
fn group_cost(group: &PredGroup, ctx: &SelCtx) -> f64 {
    if lm_cost_bytes_enabled() && !ctx.cost.is_empty() {
        let bytes: f64 = group
            .col_names
            .iter()
            .map(|n| ctx.cost.get(n).copied().unwrap_or(0.0))
            .sum();
        return bytes.max(1.0);
    }
    group.col_names.len().max(1) as f64
}

/// Estimated surviving fraction for a group = product of its conjuncts'
/// selectivities (independence assumption), clamped away from degenerate 0.
fn group_selectivity(group: &PredGroup, ctx: &SelCtx) -> f64 {
    let mut sel = 1.0f64;
    for conj in split_conjunction(&group.subpred) {
        sel *= conjunct_selectivity(&conj, ctx);
    }
    sel.clamp(0.01, 1.0)
}

/// Estimate the surviving fraction of a single `col <cmp> literal` conjunct from
/// this row group's Parquet metadata. Equality/inequality uses the column's
/// distinct_count (NDV) when present, else a string min/max range test, else a
/// constant; range comparisons interpolate over the (numeric or lexicographic)
/// min/max interval. Never panics — any unusable input yields a neutral value.
fn conjunct_selectivity(expr: &ExprRef, ctx: &SelCtx) -> f64 {
    let Expr::BinaryOp { op, left, right } = expr.as_ref() else {
        return 0.5;
    };
    // Exactly one referenced column, with a literal on the other side.
    let cols = get_required_columns(expr);
    if cols.len() != 1 {
        return 0.5;
    }
    let (lit, flip) = match (lit_of(left), lit_of(right)) {
        (None, Some(l)) => (l, false), // col <op> literal
        (Some(l), None) => (l, true),  // literal <op> col  → flip the operator
        _ => return eq_fallback(op),
    };
    let op = if flip { flip_op(*op) } else { *op };
    let col = cols[0].as_str();

    match op {
        Operator::Eq | Operator::EqNullSafe | Operator::NotEq => eq_selectivity(col, lit, op, ctx),
        _ => range_selectivity(col, lit, op, ctx),
    }
}

/// Selectivity of an equality/inequality conjunct. Prefers the column's
/// distinct_count (NDV) from Parquet stats: for a uniform column `col == v`
/// survives ~1/NDV of rows, which can be far below any min/max-based guess.
/// Falls back to a string min/max containment test, then to [`eq_fallback`].
fn eq_selectivity(col: &str, lit: &Literal, op: Operator, ctx: &SelCtx) -> f64 {
    // distinct_count (NDV): for a uniform column `col == v` survives ~1/NDV rows,
    // far below any min/max guess. Many writers omit it → fall through.
    if let Some(&ndv) = ctx.ndv.get(col)
        && ndv > 0
    {
        let frac = (1.0 / ndv as f64).clamp(0.01, 1.0);
        return match op {
            Operator::NotEq => (1.0 - frac).clamp(0.01, 1.0),
            _ => frac,
        };
    }
    // String equality: a value strictly outside [min, max] cannot occur in this
    // row group, so `==` filters everything and `!=` keeps everything. Bounds of
    // >= STR_STATS_TRUNC_LEN bytes may be Parquet-truncated prefixes → skip.
    if let Literal::Utf8(s) = lit
        && lm_str_stats_enabled()
        && let Some((min, max)) = col_min_max_str(col, ctx)
        && min.len() < STR_STATS_TRUNC_LEN
        && max.len() < STR_STATS_TRUNC_LEN
        && (s.as_str() < min.as_str() || s.as_str() > max.as_str())
    {
        return match op {
            Operator::NotEq => 1.0,
            _ => 0.01,
        };
    }
    eq_fallback(&op)
}

/// Selectivity of a range conjunct (`<`, `<=`, `>`, `>=`). Strings interpolate
/// over the lexicographic min/max interval (when the string-stats axis is on);
/// numerics over the float min/max interval. Everything else is neutral.
fn range_selectivity(col: &str, lit: &Literal, op: Operator, ctx: &SelCtx) -> f64 {
    if !matches!(
        op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return 0.5;
    }
    // String literal → estimate from the row group's lexicographic min/max so a
    // cheap, highly-selective string range can order ahead of an expensive
    // equality group. Without this, `s < 'p'` falls to a neutral 0.5 while
    // `t = 'x'` gets 0.25, wrongly ranking the expensive group first.
    if let Literal::Utf8(lit_s) = lit {
        return if lm_str_stats_enabled() {
            str_range_selectivity(col, lit_s, op, ctx)
        } else {
            eq_fallback(&op)
        };
    }

    let Some(litv) = lit_to_f64(lit) else {
        return eq_fallback(&op);
    };
    let Some((min, max)) = col_min_max(col, ctx) else {
        return eq_fallback(&op);
    };
    // Degenerate / inverted range → no usable signal.
    if !(max - min).is_normal() || max <= min {
        return 0.5;
    }
    let frac = match op {
        Operator::Lt | Operator::LtEq => (litv - min) / (max - min),
        Operator::Gt | Operator::GtEq => (max - litv) / (max - min),
        _ => return 0.5,
    };
    frac.clamp(0.01, 1.0)
}

fn lit_of(e: &ExprRef) -> Option<&Literal> {
    match e.as_ref() {
        Expr::Literal(l) => Some(l),
        _ => None,
    }
}

fn lit_to_f64(l: &Literal) -> Option<f64> {
    Some(match l {
        Literal::Int8(v) => *v as f64,
        Literal::Int16(v) => *v as f64,
        Literal::Int32(v) => *v as f64,
        Literal::Int64(v) => *v as f64,
        Literal::UInt8(v) => *v as f64,
        Literal::UInt16(v) => *v as f64,
        Literal::UInt32(v) => *v as f64,
        Literal::UInt64(v) => *v as f64,
        Literal::Float32(v) => *v as f64,
        Literal::Float64(v) => *v,
        // Decimal(value, _precision, scale) → unscaled integer / 10^scale.
        Literal::Decimal(v, _, scale) => *v as f64 / 10f64.powi(*scale as i32),
        _ => return None,
    })
}

fn col_min_max(name: &str, ctx: &SelCtx) -> Option<(f64, f64)> {
    let stats = ctx.stats?;
    let idx = ctx.schema.field_names().position(|n| n == name)?;
    match stats.index(idx) {
        ColumnRangeStatistics::Loaded(lo, hi) => {
            let lo = lo.cast(&DataType::Float64).ok()?.f64().ok()?.get(0)?;
            let hi = hi.cast(&DataType::Float64).ok()?.f64().ok()?.get(0)?;
            Some((lo, hi))
        }
        ColumnRangeStatistics::Missing => None,
    }
}

/// Utf8 min/max for `name` from the row group's [`ColumnRangeStatistics`], when
/// loaded and actually Utf8-typed. Returns owned strings (cheap: at most one per
/// group per row group).
fn col_min_max_str(name: &str, ctx: &SelCtx) -> Option<(String, String)> {
    let stats = ctx.stats?;
    let idx = ctx.schema.field_names().position(|n| n == name)?;
    match stats.index(idx) {
        ColumnRangeStatistics::Loaded(lo, hi) => {
            let lo = lo.utf8().ok()?.get(0)?.to_string();
            let hi = hi.utf8().ok()?.get(0)?.to_string();
            Some((lo, hi))
        }
        ColumnRangeStatistics::Missing => None,
    }
}

/// Project a string into `[0, 1)` by reading its first bytes as a base-256
/// fraction. Monotonic in UTF-8 byte order (which equals Unicode scalar order),
/// so a difference of two projections approximates their relative lexicographic
/// distance — enough to interpolate an interval selectivity. Only the first 8
/// bytes are used: beyond that the contribution is < 2^-64 and irrelevant to a
/// heuristic ordering decision.
fn str_to_unit(s: &str) -> f64 {
    let mut acc = 0.0f64;
    let mut scale = 1.0f64 / 256.0;
    for &b in s.as_bytes().iter().take(8) {
        acc += f64::from(b) * scale;
        scale /= 256.0;
    }
    acc
}

/// Parquet truncates string column statistics at this many bytes by default; a
/// truncated bound is a prefix of the true extreme, so interval math on it would
/// bias the estimate. Daft's [`ColumnRangeStatistics`] drops arrow-rs's
/// exactness flag, so we detect the risk by length and bail to the constant.
const STR_STATS_TRUNC_LEN: usize = 64;

/// Selectivity of `col <op> lit_s` for a Utf8 *range* conjunct, estimated from
/// the row group's lexicographic min/max. Equality is handled separately in
/// [`eq_selectivity`]. This only ever feeds group *ordering*, so an imprecise
/// estimate costs performance, never correctness; any unusable input degrades to
/// [`eq_fallback`].
fn str_range_selectivity(col: &str, lit_s: &str, op: Operator, ctx: &SelCtx) -> f64 {
    let Some((min, max)) = col_min_max_str(col, ctx) else {
        return eq_fallback(&op);
    };
    // Possibly-truncated bounds → unreliable interval → neutral constant.
    if min.len() >= STR_STATS_TRUNC_LEN || max.len() >= STR_STATS_TRUNC_LEN {
        return eq_fallback(&op);
    }
    let (lo, hi) = (str_to_unit(&min), str_to_unit(&max));
    // Degenerate / inverted range → no usable signal.
    if !(hi - lo).is_normal() || hi <= lo {
        return eq_fallback(&op);
    }
    let x = str_to_unit(lit_s);
    let frac = match op {
        Operator::Lt | Operator::LtEq => (x - lo) / (hi - lo),
        Operator::Gt | Operator::GtEq => (hi - x) / (hi - lo),
        _ => return eq_fallback(&op),
    };
    frac.clamp(0.01, 1.0)
}

/// Equality/inequality have no reliable range-fraction estimate from min/max
/// alone; use modest constants so they still participate in ordering.
fn eq_fallback(op: &Operator) -> f64 {
    match op {
        Operator::Eq | Operator::EqNullSafe => 0.25,
        Operator::NotEq => 0.75,
        _ => 0.5,
    }
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::*;
    // Production predicates reaching the pipelined path are already resolved
    // (the `predicate_pushed` guard implies `get_required_columns` succeeded), so
    // tests use `resolved_col` to match — `get_required_columns` only extracts
    // `Column::Resolved(Basic(..))`.
    use daft_dsl::{lit, resolved_col as col};
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn names(groups: &[PredGroup]) -> Vec<Vec<String>> {
        groups.iter().map(|g| g.col_names.clone()).collect()
    }

    /// Build a [`SelCtx`] with loaded min/max stats but no NDV/cost metadata
    /// (the default pyarrow-written case), for the selectivity/rank tests.
    fn ctx_from<'a>(stats: &'a TableStatistics, schema: &'a Schema) -> SelCtx<'a> {
        SelCtx {
            stats: Some(stats),
            schema,
            ndv: HashMap::new(),
            cost: HashMap::new(),
            rg_rows: 0,
        }
    }

    /// Build a [`SelCtx`] with no stats at all (neutral-fallback path).
    fn ctx_missing(schema: &Schema) -> SelCtx<'_> {
        SelCtx {
            stats: None,
            schema,
            ndv: HashMap::new(),
            cost: HashMap::new(),
            rg_rows: 0,
        }
    }

    #[test]
    fn split_flattens_nested_and() {
        // (a > 5 AND b < 10) AND c == 1  →  three conjuncts
        let e = col("a")
            .gt(lit(5))
            .and(col("b").lt(lit(10)))
            .and(col("c").eq(lit(1)));
        let splits = split_conjunction(&e);
        assert_eq!(splits.len(), 3, "nested AND should flatten to 3 conjuncts");

        // A single non-AND predicate is one conjunct.
        let single = col("a").gt(lit(5));
        assert_eq!(split_conjunction(&single).len(), 1);
    }

    #[test]
    fn groups_are_column_disjoint() {
        // a>5 AND b<10  →  two disjoint groups.
        let e = col("a").gt(lit(5)).and(col("b").lt(lit(10)));
        let groups = group_conjuncts(split_conjunction(&e));
        assert_eq!(groups.len(), 2);
        let mut ns = names(&groups);
        ns.sort();
        assert_eq!(ns, vec![vec!["a".to_string()], vec!["b".to_string()]]);
    }

    #[test]
    fn overlapping_columns_merge_into_one_group() {
        // a>5 AND a<100  →  share `a`  →  one group.
        let e = col("a").gt(lit(5)).and(col("a").lt(lit(100)));
        let groups = group_conjuncts(split_conjunction(&e));
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].col_names, vec!["a".to_string()]);
    }

    #[test]
    fn bridging_conjunct_merges_transitively() {
        // a>5 AND b<10 AND (a < b)  →  the third references both a and b,
        // bridging {a} and {b} into a single group.
        let e = col("a")
            .gt(lit(5))
            .and(col("b").lt(lit(10)))
            .and(col("a").lt(col("b")));
        let groups = group_conjuncts(split_conjunction(&e));
        assert_eq!(groups.len(), 1, "bridging conjunct must merge both groups");
        assert_eq!(groups[0].col_names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn plan_returns_none_for_single_group() {
        // Not enough disjoint groups to pipeline.
        let single = col("a").gt(lit(5));
        assert!(plan_pred_groups(&single).is_none());

        let overlapping = col("a").gt(lit(5)).and(col("a").lt(lit(100)));
        assert!(plan_pred_groups(&overlapping).is_none());

        let pipelineable = col("a").gt(lit(5)).and(col("b").lt(lit(10)));
        assert!(plan_pred_groups(&pipelineable).is_some());
    }

    #[test]
    fn three_way_split_groups_correctly() {
        // a>5 AND b<10 AND a<100 AND c==1
        //   → {a: a>5, a<100}, {b: b<10}, {c: c==1}
        let e = col("a")
            .gt(lit(5))
            .and(col("b").lt(lit(10)))
            .and(col("a").lt(lit(100)))
            .and(col("c").eq(lit(1)));
        let groups = group_conjuncts(split_conjunction(&e));
        assert_eq!(groups.len(), 3);
        let mut ns = names(&groups);
        ns.sort();
        assert_eq!(
            ns,
            vec![
                vec!["a".to_string()],
                vec!["b".to_string()],
                vec!["c".to_string()]
            ]
        );
        // The `a` group holds both a-conjuncts ANDed together.
        let a_group = groups
            .iter()
            .find(|g| g.col_names == vec!["a".to_string()])
            .unwrap();
        assert_eq!(split_conjunction(&a_group.subpred).len(), 2);
    }

    #[test]
    fn selectivity_orders_most_selective_first() {
        // Both columns span [0, 1000].
        //   `a > 900` keeps ~10%   → more selective → must rank first.
        //   `b < 900` keeps ~90%.
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[0, 1000]).into_series(),
            Int64Array::from_slice("b", &[0, 1000]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_table(&table);
        let schema = table.schema.as_ref();
        let ctx = ctx_from(&stats, schema);

        let b_group = PredGroup {
            subpred: col("b").lt(lit(900)),
            col_names: vec!["b".into()],
        };
        let a_group = PredGroup {
            subpred: col("a").gt(lit(900)),
            col_names: vec!["a".into()],
        };

        let sel_a = group_selectivity(&a_group, &ctx);
        let sel_b = group_selectivity(&b_group, &ctx);
        assert!(
            (sel_a - 0.1).abs() < 1e-9,
            "a>900 over [0,1000] should estimate ~0.1, got {sel_a}"
        );
        assert!(
            (sel_b - 0.9).abs() < 1e-9,
            "b<900 over [0,1000] should estimate ~0.9, got {sel_b}"
        );

        let rank_a = group_rank(&a_group, &ctx);
        let rank_b = group_rank(&b_group, &ctx);
        assert!(
            rank_a < rank_b,
            "more-selective a>900 rank {rank_a} should sort before b<900 rank {rank_b}"
        );
    }

    #[test]
    fn selectivity_missing_stats_is_neutral() {
        // No stats → neutral 0.5 for a range predicate (fallback path, no panic).
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[0, 1000]).into_series(),
        ])
        .unwrap();
        let schema = table.schema.as_ref();
        let g = PredGroup {
            subpred: col("a").gt(lit(900)),
            col_names: vec!["a".into()],
        };
        let ctx = ctx_missing(schema);
        let sel = group_selectivity(&g, &ctx);
        assert!(
            (sel - 0.5).abs() < 1e-9,
            "no stats → neutral 0.5, got {sel}"
        );
    }

    #[test]
    fn str_range_orders_before_equality() {
        // `s < "k01000"` over s∈["k00000","k99999"] is highly selective; a naive
        // estimate would give the string range a neutral 0.5 and rank it AFTER
        // the 0.25 equality on `t`. Stats-based string selectivity must flip
        // that so the cheap selective range decodes first.
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["k00000", "k99999"]).into_series(),
            Utf8Array::from_slice("t", &["aaa", "zzz"]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_table(&table);
        let schema = table.schema.as_ref();
        let ctx = ctx_from(&stats, schema);

        let s_group = PredGroup {
            subpred: col("s").lt(lit("k01000")),
            col_names: vec!["s".into()],
        };
        let t_group = PredGroup {
            subpred: col("t").eq(lit("mmm")),
            col_names: vec!["t".into()],
        };

        let sel_s = group_selectivity(&s_group, &ctx);
        assert!(
            sel_s < 0.25,
            "selective string range should estimate below the 0.25 equality constant, got {sel_s}"
        );
        let rank_s = group_rank(&s_group, &ctx);
        let rank_t = group_rank(&t_group, &ctx);
        assert!(
            rank_s < rank_t,
            "cheap selective string range (rank {rank_s}) must order before the equality group (rank {rank_t})"
        );
    }

    #[test]
    fn str_eq_outside_range_is_very_selective() {
        // "zzzzzzz" > max "k99999" → cannot occur in this row group → ~0.01.
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["k00000", "k99999"]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_table(&table);
        let schema = table.schema.as_ref();
        let ctx = ctx_from(&stats, schema);
        let g = PredGroup {
            subpred: col("s").eq(lit("zzzzzzz")),
            col_names: vec!["s".into()],
        };
        let sel = group_selectivity(&g, &ctx);
        assert!(
            (sel - 0.01).abs() < 1e-9,
            "value above max cannot be present → ~0.01, got {sel}"
        );
    }

    #[test]
    fn str_truncated_stats_fall_back_to_constant() {
        // A >= 64-byte max may be a Parquet-truncated prefix → interval math is
        // unreliable → fall back to the neutral constant (0.5 for Lt).
        let long_max = "x".repeat(70);
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["a", long_max.as_str()]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_table(&table);
        let schema = table.schema.as_ref();
        let ctx = ctx_from(&stats, schema);
        let g = PredGroup {
            subpred: col("s").lt(lit("m")),
            col_names: vec!["s".into()],
        };
        let sel = group_selectivity(&g, &ctx);
        assert!(
            (sel - 0.5).abs() < 1e-9,
            "possibly-truncated string max → neutral fallback, got {sel}"
        );
    }

    #[test]
    fn ndv_equality_is_highly_selective() {
        // distinct_count (NDV) drives equality selectivity: a high-cardinality
        // column's `== v` survives ~1/NDV of rows (clamped to 0.01), far below a
        // low-cardinality column's. Stats are irrelevant here (NDV short-circuits).
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[0, 1000]).into_series(),
            Int64Array::from_slice("b", &[0, 1000]).into_series(),
        ])
        .unwrap();
        let schema = table.schema.as_ref();
        let mut ndv = HashMap::new();
        ndv.insert("a".to_string(), 100_000usize); // very high cardinality
        ndv.insert("b".to_string(), 4usize); // low cardinality
        let ctx = SelCtx {
            stats: None,
            schema,
            ndv,
            cost: HashMap::new(),
            rg_rows: 0,
        };

        let a_group = PredGroup {
            subpred: col("a").eq(lit(5)),
            col_names: vec!["a".into()],
        };
        let b_group = PredGroup {
            subpred: col("b").eq(lit(2)),
            col_names: vec!["b".into()],
        };
        let sel_a = group_selectivity(&a_group, &ctx);
        let sel_b = group_selectivity(&b_group, &ctx);
        assert!(
            (sel_a - 0.01).abs() < 1e-9,
            "high-NDV equality → ~1/NDV clamped to 0.01, got {sel_a}"
        );
        assert!(
            (sel_b - 0.25).abs() < 1e-9,
            "NDV=4 equality → 1/4 = 0.25, got {sel_b}"
        );
        assert!(
            group_rank(&a_group, &ctx) < group_rank(&b_group, &ctx),
            "high-NDV equality (more selective) must order before low-NDV equality"
        );
    }

    #[test]
    fn cost_from_bytes_orders_cheap_first() {
        // Two columns with identical ranges and identical `> 900` predicates have
        // equal selectivity, so the *cost* axis must break the tie: the cheaper
        // column (fewer bytes to decode) ranks first.
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("cheap", &[0, 1000]).into_series(),
            Int64Array::from_slice("pricey", &[0, 1000]).into_series(),
        ])
        .unwrap();
        let stats = TableStatistics::from_table(&table);
        let schema = table.schema.as_ref();
        let mut cost = HashMap::new();
        cost.insert("cheap".to_string(), 100.0f64);
        cost.insert("pricey".to_string(), 1_000_000.0f64);
        let ctx = SelCtx {
            stats: Some(&stats),
            schema,
            ndv: HashMap::new(),
            cost,
            rg_rows: 0,
        };

        let cheap_group = PredGroup {
            subpred: col("cheap").gt(lit(900)),
            col_names: vec!["cheap".into()],
        };
        let pricey_group = PredGroup {
            subpred: col("pricey").gt(lit(900)),
            col_names: vec!["pricey".into()],
        };
        let sel_c = group_selectivity(&cheap_group, &ctx);
        let sel_p = group_selectivity(&pricey_group, &ctx);
        assert!(
            (sel_c - sel_p).abs() < 1e-9,
            "identical predicates → equal selectivity, got {sel_c} vs {sel_p}"
        );
        assert!(
            group_cost(&cheap_group, &ctx) < group_cost(&pricey_group, &ctx),
            "cost axis should reflect the byte-size difference"
        );
        assert!(
            group_rank(&cheap_group, &ctx) < group_rank(&pricey_group, &ctx),
            "equal selectivity → cheaper column orders first"
        );
    }
}
