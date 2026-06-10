//! Execution-time clustering for pipeline nodes.
//!
//! [`BoundClusteringSpec`] is the bound (`T = BoundExpr`) instantiation of the generic
//! [`daft_logical_plan::partitioning::ClusteringSpec`]. The logical plan only ever deals with the
//! resolved-by-name form (`ClusteringSpec`); binding happens here, at the logical -> pipeline
//! boundary, so every downstream check (`can_skip_hash_repartition`, join co-partitioning) can compare
//! bound expressions directly instead of re-binding on the fly.
//!
//! Pipeline nodes never call these helpers directly. Every node establishes its output clustering
//! by passing a [`ClusteringStrategy`](super::ClusteringStrategy) to `PipelineNodeConfig::new`,
//! which routes to the functions below. That keeps the choice explicit and impossible to forget —
//! see the `ClusteringStrategy` docs.

use std::collections::HashMap;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{Column, Expr, ExprRef, bound_col, expr::bound_expr::BoundExpr};
use daft_logical_plan::partitioning::{
    ClusteringSpec, HashClusteringConfig, RandomClusteringConfig, RangeClusteringConfig,
    RepartitionSpec, UnknownClusteringConfig,
};
use daft_schema::schema::Schema;

/// Execution-time clustering of a pipeline node's output, expressed entirely in **bound**
/// expressions. See the module docs for the difference between the logical and bound forms.
pub(crate) type BoundClusteringSpec = ClusteringSpec<BoundExpr>;

/// Builds a bound clustering spec from a logical [`RepartitionSpec`], binding any unbound
/// (resolved-by-name) keys against `schema`. This is the logical -> pipeline boundary for
/// clustering produced by an explicit repartition; pass the result as
/// [`ClusteringStrategy::Explicit`](super::ClusteringStrategy::Explicit).
pub(super) fn clustering_from_repartition_spec(
    spec: &RepartitionSpec,
    num_partitions: usize,
    schema: &Schema,
) -> DaftResult<BoundClusteringSpec> {
    Ok(match spec {
        RepartitionSpec::Hash(c) => BoundClusteringSpec::Hash(HashClusteringConfig::new(
            c.num_partitions.unwrap_or(num_partitions),
            BoundExpr::bind_all(&c.by, schema)?,
        )),
        RepartitionSpec::Range(c) => BoundClusteringSpec::Range(RangeClusteringConfig::new(
            c.num_partitions.unwrap_or(num_partitions),
            // Range repartition keys are already bound.
            c.by.clone(),
            c.descending.clone(),
        )),
        RepartitionSpec::Random(c) => BoundClusteringSpec::Random(RandomClusteringConfig::new(
            c.num_partitions.unwrap_or(num_partitions),
        )),
    })
}

/// Propagates `input`'s clustering through a projection, producing the output's clustering. This
/// backs [`ClusteringStrategy::Projection`](super::ClusteringStrategy::Projection).
///
/// A clustering key survives if every column it references is still produced by the projection
/// (passed through, or as part of a larger expression the projection materializes); otherwise the
/// spec downgrades to `Unknown`. Both `input` and `projection` are bound against the input schema;
/// output keys are bound against `output_schema`.
///
/// # How it works
///
/// Clustering keys are bound against the *input* schema, but the output's clustering must be
/// expressed against the *output* schema. So we build a lookup from "expression the projection
/// computes" → "output column that holds it", then rewrite each clustering key through it.
///
/// Worked example — input clustered by `[col#0, col#1 % 4]`, projection
/// `[col#0, col#2 AS "v", (col#1 % 4) AS "bucket"]` producing output schema `[k, v, bucket]`:
///
/// ```text
///   projection entry            map key (alias stripped)   →   output column
///   ----------------            ------------------------       -------------
///   col#0                       col#0                      →   col#0   (passthrough "k")
///   col#2 AS "v"                col#2                      →   col#1   ("v")
///   (col#1 % 4) AS "bucket"     col#1 % 4                  →   col#2   ("bucket")
///
///   clustering key   lookup / rewrite                       result
///   --------------   ----------------                       ------
///   col#0            matches map key col#0                  col#0
///   col#1 % 4        matches map key col#1 % 4              col#2
///
///   => output clustering = Hash([col#0, col#2])
/// ```
///
/// ## Why the alias is stripped from the map key
///
/// A projection entry is `<expr> AS <name>`, carrying two separate facts: the *value* it computes
/// (`<expr>`) and the *output column* it lands in (`<name>`). A clustering key is a bare partition
/// expression with no alias (e.g. `col#1 % 4`). To match it we key the map on the alias-stripped
/// expression — `Alias(col#1 % 4, "bucket")` would never structurally equal the bare key
/// `col#1 % 4`, so keeping the alias would miss the match and force a needless downgrade to
/// `Unknown`. The alias is still used, via `.name()`, to locate the output column the key is
/// rewritten *to*.
pub(super) fn translate_clustering_through_projection(
    input: &BoundClusteringSpec,
    projection: &[BoundExpr],
    output_schema: &Schema,
) -> BoundClusteringSpec {
    let num_partitions = input.num_partitions();
    let by = match input {
        BoundClusteringSpec::Random(_) | BoundClusteringSpec::Unknown(_) => return input.clone(),
        BoundClusteringSpec::Hash(HashClusteringConfig { by, .. })
        | BoundClusteringSpec::Range(RangeClusteringConfig { by, .. }) => by,
    };

    // Build the "computed expression -> output column" lookup. The key is the projected
    // expression with its outer alias stripped (clustering keys are alias-free, so this is the
    // form they must match against); the value is the bound output column, located via the
    // entry's output name (which *does* honor the alias).
    let mut projected_to_output: HashMap<ExprRef, ExprRef> = HashMap::new();
    for proj in projection {
        let (inner, _) = proj.inner().unwrap_alias();
        let Ok(out_index) = output_schema.get_index(proj.inner().name()) else {
            continue;
        };
        let out_col = bound_col(out_index, output_schema[out_index].clone());
        // First projection of a given inner expression wins. In the degenerate case where the
        // same sub-expression is projected twice (e.g. once as a passthrough and once under an
        // alias), this keys the clustering off whichever appears first — both reference the
        // same value, so the choice only affects which output column name the key adopts.
        projected_to_output.entry(inner).or_insert(out_col);
    }

    let translated: Option<Vec<BoundExpr>> = by
        .iter()
        .map(|key| {
            translate_bound_clustering_key(key.inner(), &projected_to_output)
                .map(BoundExpr::new_unchecked)
        })
        .collect();

    match translated {
        None => BoundClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)),
        Some(new_by) => match input {
            BoundClusteringSpec::Range(RangeClusteringConfig { descending, .. }) => {
                BoundClusteringSpec::Range(RangeClusteringConfig::new(
                    num_partitions,
                    new_by,
                    descending.clone(),
                ))
            }
            _ => BoundClusteringSpec::Hash(HashClusteringConfig::new(num_partitions, new_by)),
        },
    }
}

/// Rewrites a bound clustering-key expression so its column references point at the projection's
/// output columns. Returns `None` if the key references an input column the projection drops.
fn translate_bound_clustering_key(
    key: &ExprRef,
    projected_to_output: &HashMap<ExprRef, ExprRef>,
) -> Option<ExprRef> {
    let mut dropped_column = false;
    let result = key
        .clone()
        .transform_down(|e| {
            // If this whole sub-expression is materialized as an output column, reference that
            // column and stop descending. Handles passthrough columns and aliased computed
            // expressions alike (e.g. `f(col("id")) AS h` lets a key `f(col("id"))` become
            // `col("h")`).
            if let Some(out_col) = projected_to_output.get(&e) {
                return Ok(Transformed::new(
                    out_col.clone(),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            // A bound column not materialized in the output can't be expressed after the
            // projection.
            if matches!(e.as_ref(), Expr::Column(Column::Bound(_))) {
                dropped_column = true;
                return Ok(Transformed::new(e, false, TreeNodeRecursion::Stop));
            }
            Ok(Transformed::no(e))
        })
        .expect("clustering key rewrite is infallible");
    (!dropped_column).then_some(result.data)
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, Operator};
    use daft_dsl::{binary_op, bound_col, lit};
    use daft_schema::{field::Field, schema::Schema};

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        let fields: Vec<Field> = fields
            .iter()
            .map(|(n, d)| Field::new(*n, d.clone()))
            .collect();
        Schema::new(fields)
    }

    fn bound(index: usize, name: &str, dtype: DataType) -> ExprRef {
        bound_col(index, Field::new(name, dtype))
    }

    fn bexpr(e: ExprRef) -> BoundExpr {
        BoundExpr::new_unchecked(e)
    }

    fn hour_of(id: ExprRef) -> ExprRef {
        binary_op(Operator::FloorDivide, id, lit(3_600_000i64))
    }

    fn mod_of(e: ExprRef) -> ExprRef {
        binary_op(Operator::Modulus, e, lit(100i64))
    }

    fn keys(spec: &BoundClusteringSpec) -> Vec<ExprRef> {
        spec.partition_by()
            .iter()
            .map(|e| e.inner().clone())
            .collect()
    }

    #[test]
    fn passthrough_and_expression_alias() {
        let output_schema = schema(&[
            ("producer", DataType::Utf8),
            ("id", DataType::Int64),
            ("ts", DataType::Int64),
            ("h", DataType::Int64),
        ]);
        let clustering = BoundClusteringSpec::hash(
            4,
            vec![
                bexpr(bound(0, "producer", DataType::Utf8)),
                bexpr(hour_of(bound(1, "id", DataType::Int64))),
            ],
        );
        let projection = vec![
            bexpr(bound(0, "producer", DataType::Utf8)),
            bexpr(bound(1, "id", DataType::Int64)),
            bexpr(bound(2, "ts", DataType::Int64)),
            bexpr(hour_of(bound(1, "id", DataType::Int64)).alias("h")),
        ];
        let out = translate_clustering_through_projection(&clustering, &projection, &output_schema);
        assert_eq!(
            keys(&out),
            vec![
                bound(0, "producer", DataType::Utf8),
                bound(3, "h", DataType::Int64),
            ]
        );
    }

    #[test]
    fn downgrades_to_unknown_when_column_dropped() {
        let output_schema = schema(&[("other", DataType::Int64)]);
        let clustering =
            BoundClusteringSpec::hash(4, vec![bexpr(hour_of(bound(0, "id", DataType::Int64)))]);
        let projection = vec![bexpr(bound(1, "other", DataType::Int64))];
        let out = translate_clustering_through_projection(&clustering, &projection, &output_schema);
        assert!(matches!(out, BoundClusteringSpec::Unknown(_)));
    }

    #[test]
    fn non_injective_projection_does_not_fabricate_clustering() {
        // Hash([a]) + (mod(a) AS b), a dropped => Unknown, never Hash([b]).
        let output_schema = schema(&[("b", DataType::Int64)]);
        let clustering = BoundClusteringSpec::hash(4, vec![bexpr(bound(0, "a", DataType::Int64))]);
        let projection = vec![bexpr(mod_of(bound(0, "a", DataType::Int64)).alias("b"))];
        let out = translate_clustering_through_projection(&clustering, &projection, &output_schema);
        assert!(matches!(out, BoundClusteringSpec::Unknown(_)));
    }

    #[test]
    fn clustering_follows_key_not_non_injective_derivative() {
        // Hash([a]) + (a, mod(a) AS b) => Hash([a]), never Hash([b]).
        let output_schema = schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let clustering = BoundClusteringSpec::hash(4, vec![bexpr(bound(0, "a", DataType::Int64))]);
        let projection = vec![
            bexpr(bound(0, "a", DataType::Int64)),
            bexpr(mod_of(bound(0, "a", DataType::Int64)).alias("b")),
        ];
        let out = translate_clustering_through_projection(&clustering, &projection, &output_schema);
        assert_eq!(keys(&out), vec![bound(0, "a", DataType::Int64)]);
    }
}
