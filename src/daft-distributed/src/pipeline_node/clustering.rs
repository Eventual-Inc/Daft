use std::collections::HashMap;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{Column, Expr, ExprRef, bound_col, expr::bound_expr::BoundExpr};
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_schema::schema::Schema;

/// Execution-time clustering of a pipeline node's output, expressed entirely in **bound**
/// expressions.
///
/// Unlike the logical [`daft_logical_plan::partitioning::ClusteringSpec`], which is schema-agnostic
/// (resolved-by-name) so it can survive logical-plan rewrites, the pipeline operates on a fixed,
/// post-optimization schema where columns are referenced by bound index. Binding the clustering
/// keys once — at the point each pipeline node is built — lets every downstream check
/// (`needs_hash_repartition`, join co-partitioning) compare bound expressions directly instead of
/// re-binding on the fly.
#[derive(Clone, Debug)]
pub(crate) enum BoundClusteringSpec {
    Hash {
        num_partitions: usize,
        by: Vec<BoundExpr>,
    },
    Range {
        num_partitions: usize,
        by: Vec<BoundExpr>,
        descending: Vec<bool>,
    },
    Random {
        num_partitions: usize,
    },
    Unknown {
        num_partitions: usize,
    },
}

impl BoundClusteringSpec {
    pub fn unknown(num_partitions: usize) -> Self {
        Self::Unknown { num_partitions }
    }

    pub fn hash(num_partitions: usize, by: Vec<BoundExpr>) -> Self {
        Self::Hash { num_partitions, by }
    }

    pub fn num_partitions(&self) -> usize {
        match self {
            Self::Hash { num_partitions, .. }
            | Self::Range { num_partitions, .. }
            | Self::Random { num_partitions }
            | Self::Unknown { num_partitions } => *num_partitions,
        }
    }

    /// The clustering keys, or an empty slice for `Random` / `Unknown`.
    pub fn partition_by(&self) -> &[BoundExpr] {
        match self {
            Self::Hash { by, .. } | Self::Range { by, .. } => by,
            Self::Random { .. } | Self::Unknown { .. } => &[],
        }
    }

    pub fn is_hash(&self) -> bool {
        matches!(self, Self::Hash { .. })
    }

    /// Builds a bound clustering spec from a logical [`RepartitionSpec`], binding any unbound
    /// (resolved-by-name) keys against `schema`. This is the logical -> pipeline boundary for
    /// clustering produced by an explicit repartition.
    pub fn from_repartition_spec(
        spec: &RepartitionSpec,
        num_partitions: usize,
        schema: &Schema,
    ) -> DaftResult<Self> {
        Ok(match spec {
            RepartitionSpec::Hash(c) => Self::Hash {
                num_partitions: c.num_partitions.unwrap_or(num_partitions),
                by: BoundExpr::bind_all(&c.by, schema)?,
            },
            RepartitionSpec::Range(c) => Self::Range {
                num_partitions: c.num_partitions.unwrap_or(num_partitions),
                // Range repartition keys are already bound.
                by: c.by.clone(),
                descending: c.descending.clone(),
            },
            RepartitionSpec::Random(c) => Self::Random {
                num_partitions: c.num_partitions.unwrap_or(num_partitions),
            },
        })
    }

    /// Propagates this clustering through a projection, producing the output's clustering.
    ///
    /// A clustering key survives if every column it references is still produced by the projection
    /// (passed through, or as part of a larger expression the projection materializes); otherwise
    /// the spec downgrades to `Unknown`. Both `self` and `projection` are bound against the input
    /// schema; output keys are bound against `output_schema`.
    pub fn translate_through_projection(
        &self,
        projection: &[BoundExpr],
        output_schema: &Schema,
    ) -> Self {
        let num_partitions = self.num_partitions();
        let by = match self {
            Self::Random { .. } | Self::Unknown { .. } => return self.clone(),
            Self::Hash { by, .. } | Self::Range { by, .. } => by,
        };

        // Map each projected expression (outer alias stripped) to the output column it
        // materializes. Both the projection and the clustering keys reference the input schema, so
        // a surviving key matches one of these entries structurally.
        let mut projected_to_output: HashMap<ExprRef, ExprRef> = HashMap::new();
        for proj in projection {
            let (inner, _) = proj.inner().unwrap_alias();
            let Ok(out_index) = output_schema.get_index(proj.inner().name()) else {
                continue;
            };
            let out_col = bound_col(out_index, output_schema[out_index].clone());
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
            None => Self::Unknown { num_partitions },
            Some(new_by) => match self {
                Self::Range { descending, .. } => Self::Range {
                    num_partitions,
                    by: new_by,
                    descending: descending.clone(),
                },
                _ => Self::Hash {
                    num_partitions,
                    by: new_by,
                },
            },
        }
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
        let out = clustering.translate_through_projection(&projection, &output_schema);
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
        let out = clustering.translate_through_projection(&projection, &output_schema);
        assert!(matches!(out, BoundClusteringSpec::Unknown { .. }));
    }

    #[test]
    fn non_injective_projection_does_not_fabricate_clustering() {
        // Hash([a]) + (mod(a) AS b), a dropped => Unknown, never Hash([b]).
        let output_schema = schema(&[("b", DataType::Int64)]);
        let clustering = BoundClusteringSpec::hash(4, vec![bexpr(bound(0, "a", DataType::Int64))]);
        let projection = vec![bexpr(mod_of(bound(0, "a", DataType::Int64)).alias("b"))];
        let out = clustering.translate_through_projection(&projection, &output_schema);
        assert!(matches!(out, BoundClusteringSpec::Unknown { .. }));
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
        let out = clustering.translate_through_projection(&projection, &output_schema);
        assert_eq!(keys(&out), vec![bound(0, "a", DataType::Int64)]);
    }
}
