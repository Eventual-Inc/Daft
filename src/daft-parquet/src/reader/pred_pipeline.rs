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

use std::{collections::HashSet, ops::Index, sync::OnceLock};

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

/// Order `groups` for a specific row group by an estimated rank, cheapest-and-
/// most-selective first. Returns group indices into `groups` (a permutation).
///
/// Rank is the classic predicate-ordering heuristic `(selectivity - 1) / cost`:
/// more negative = narrow more rows per unit of decode work = go first. Ties keep
/// the caller's original order (stable sort).
pub(super) fn order_groups(
    groups: &[PredGroup],
    rg_meta: &RowGroupMetaData,
    schema: &Schema,
) -> Vec<usize> {
    let stats = row_group_metadata_to_table_stats(rg_meta, schema).ok();
    let stats = stats.as_ref();

    let mut order: Vec<usize> = (0..groups.len()).collect();
    order.sort_by(|&a, &b| {
        let ra = group_rank(&groups[a], stats, schema);
        let rb = group_rank(&groups[b], stats, schema);
        ra.partial_cmp(&rb).unwrap_or(std::cmp::Ordering::Equal)
    });
    order
}

fn group_rank(group: &PredGroup, stats: Option<&TableStatistics>, schema: &Schema) -> f64 {
    let sel = group_selectivity(group, stats, schema);
    // Decode cost proxy: number of physical columns in the group (>= 1).
    let cost = group.col_names.len().max(1) as f64;
    (sel - 1.0) / cost
}

/// Estimated surviving fraction for a group = product of its conjuncts'
/// selectivities (independence assumption), clamped away from degenerate 0.
fn group_selectivity(group: &PredGroup, stats: Option<&TableStatistics>, schema: &Schema) -> f64 {
    let mut sel = 1.0f64;
    for conj in split_conjunction(&group.subpred) {
        sel *= conjunct_selectivity(&conj, stats, schema);
    }
    sel.clamp(0.01, 1.0)
}

/// Estimate the surviving fraction of a single `col <cmp> literal` conjunct from
/// the row group's min/max stats. Only numeric range comparisons are estimated
/// precisely; everything else (equality, strings, missing stats, non-numeric)
/// falls back to a constant. Never panics — any failure yields a neutral value.
fn conjunct_selectivity(expr: &ExprRef, stats: Option<&TableStatistics>, schema: &Schema) -> f64 {
    let stats = match stats {
        Some(s) => s,
        None => return 0.5,
    };
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
    let Some(litv) = lit_to_f64(lit) else {
        return eq_fallback(op);
    };
    let Some((min, max)) = col_min_max(&cols[0], stats, schema) else {
        return eq_fallback(op);
    };
    // Degenerate / inverted range → no usable signal.
    if !(max - min).is_normal() || max <= min {
        return 0.5;
    }
    let op = if flip { flip_op(*op) } else { *op };
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

fn col_min_max(name: &str, stats: &TableStatistics, schema: &Schema) -> Option<(f64, f64)> {
    let idx = schema.field_names().position(|n| n == name)?;
    match stats.index(idx) {
        ColumnRangeStatistics::Loaded(lo, hi) => {
            let lo = lo.cast(&DataType::Float64).ok()?.f64().ok()?.get(0)?;
            let hi = hi.cast(&DataType::Float64).ok()?.f64().ok()?.get(0)?;
            Some((lo, hi))
        }
        ColumnRangeStatistics::Missing => None,
    }
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

        let b_group = PredGroup {
            subpred: col("b").lt(lit(900)),
            col_names: vec!["b".into()],
        };
        let a_group = PredGroup {
            subpred: col("a").gt(lit(900)),
            col_names: vec!["a".into()],
        };

        let sel_a = group_selectivity(&a_group, Some(&stats), schema);
        let sel_b = group_selectivity(&b_group, Some(&stats), schema);
        assert!(
            (sel_a - 0.1).abs() < 1e-9,
            "a>900 over [0,1000] should estimate ~0.1, got {sel_a}"
        );
        assert!(
            (sel_b - 0.9).abs() < 1e-9,
            "b<900 over [0,1000] should estimate ~0.9, got {sel_b}"
        );

        let rank_a = group_rank(&a_group, Some(&stats), schema);
        let rank_b = group_rank(&b_group, Some(&stats), schema);
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
        let sel = group_selectivity(&g, None, schema);
        assert!(
            (sel - 0.5).abs() < 1e-9,
            "no stats → neutral 0.5, got {sel}"
        );
    }
}
