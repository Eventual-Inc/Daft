use std::{collections::HashMap, fmt::Display, sync::Arc};

use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{Column, Expr, ExprRef, bound_col, expr::bound_expr::BoundExpr};
use daft_recordbatch::RecordBatch;
use daft_schema::schema::Schema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Repartitioning specification.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RepartitionSpec {
    Hash(HashRepartitionConfig),
    Random(RandomShuffleConfig),
    Range(RangeRepartitionConfig),
}

impl RepartitionSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::Range(_) => "Range",
        }
    }

    pub fn repartition_by(&self) -> Vec<ExprRef> {
        match self {
            Self::Hash(HashRepartitionConfig { by, .. }) => by.clone(),
            _ => vec![],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Hash(conf) => conf.multiline_display(),
            Self::Random(conf) => conf.multiline_display(),
            Self::Range(conf) => conf.multiline_display(),
        }
    }

    pub fn to_clustering_spec(&self, upstream_num_partitions: usize) -> ClusteringSpec {
        match self {
            Self::Hash(HashRepartitionConfig { num_partitions, by }) => {
                ClusteringSpec::Hash(HashClusteringConfig::new(
                    num_partitions.unwrap_or(upstream_num_partitions),
                    by.clone(),
                ))
            }
            Self::Random(RandomShuffleConfig { num_partitions, .. }) => ClusteringSpec::Random(
                RandomClusteringConfig::new(num_partitions.unwrap_or(upstream_num_partitions)),
            ),
            Self::Range(RangeRepartitionConfig {
                num_partitions,
                by,
                descending,
                ..
            }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                num_partitions.unwrap_or(upstream_num_partitions),
                by.iter().map(|e| e.inner().clone()).collect(),
                descending.clone(),
            )),
        }
    }

    pub fn compact_display(&self) -> String {
        fn format_num_partitions(num_partitions: Option<usize>) -> String {
            num_partitions
                .map(|value| value.to_string())
                .unwrap_or_else(|| "auto".to_string())
        }

        fn format_list<T: Display>(items: impl IntoIterator<Item = T>) -> String {
            format!(
                "[{}]",
                items.into_iter().map(|item| item.to_string()).join(", ")
            )
        }

        match self {
            Self::Hash(HashRepartitionConfig { num_partitions, by }) => format!(
                "Hash (num_partitions={}, by={})",
                format_num_partitions(*num_partitions),
                format_list(by.iter().map(|expr| expr.to_string()))
            ),
            Self::Random(RandomShuffleConfig {
                num_partitions,
                seed,
            }) => {
                let mut parts = vec![format!(
                    "num_partitions={}",
                    format_num_partitions(*num_partitions)
                )];
                if let Some(seed) = seed {
                    parts.push(format!("seed={seed}"));
                }
                format!("Random ({})", parts.join(", "))
            }
            Self::Range(RangeRepartitionConfig {
                num_partitions,
                by,
                descending,
                ..
            }) => {
                let mut parts = vec![
                    format!("num_partitions={}", format_num_partitions(*num_partitions)),
                    format!("by={}", format_list(by.iter().map(|expr| expr.to_string()))),
                ];

                if !descending.is_empty() {
                    parts.push(format!("descending={}", format_list(descending.iter())));
                }
                format!("Range ({})", parts.join(", "))
            }
        }
    }
}

impl Display for RepartitionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.compact_display())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct HashRepartitionConfig {
    pub num_partitions: Option<usize>,
    pub by: Vec<ExprRef>,
}

impl HashRepartitionConfig {
    pub fn new(num_partitions: Option<usize>, by: Vec<ExprRef>) -> Self {
        Self { num_partitions, by }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Num partitions = {:?}", self.num_partitions));
        res.push(format!(
            "By = {}",
            self.by.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RandomShuffleConfig {
    pub num_partitions: Option<usize>,
    pub seed: Option<u64>,
}

impl RandomShuffleConfig {
    pub fn new(num_partitions: Option<usize>) -> Self {
        Self {
            num_partitions,
            seed: None,
        }
    }

    pub fn new_with_seed(num_partitions: Option<usize>, seed: Option<u64>) -> Self {
        Self {
            num_partitions,
            seed,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {:?}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RangeRepartitionConfig {
    pub num_partitions: Option<usize>,
    pub boundaries: RecordBatch,
    pub by: Vec<BoundExpr>,
    pub descending: Vec<bool>,
}

impl RangeRepartitionConfig {
    pub fn new(
        num_partitions: Option<usize>,
        boundaries: RecordBatch,
        by: Vec<BoundExpr>,
        descending: Vec<bool>,
    ) -> Self {
        Self {
            num_partitions,
            boundaries,
            by,
            descending,
        }
    }
}

impl RangeRepartitionConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        let pairs = self
            .by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .join(", ");
        res.push(format!("Num partitions = {:?}", self.num_partitions));
        res.push(format!("By = {}", pairs));
        res
    }
}

/// Partition scheme for Daft DataFrame.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringSpec {
    Range(RangeClusteringConfig),
    Hash(HashClusteringConfig),
    Random(RandomClusteringConfig),
    Unknown(UnknownClusteringConfig),
}

pub type ClusteringSpecRef = Arc<ClusteringSpec>;
impl ClusteringSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Range(_) => "Range",
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::Unknown(_) => "Unknown",
        }
    }

    pub fn num_partitions(&self) -> usize {
        match self {
            Self::Range(RangeClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Hash(HashClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Random(RandomClusteringConfig { num_partitions, .. }) => *num_partitions,
            Self::Unknown(UnknownClusteringConfig { num_partitions, .. }) => *num_partitions,
        }
    }

    pub fn partition_by(&self) -> Vec<ExprRef> {
        match self {
            Self::Range(RangeClusteringConfig { by, .. }) => by.clone(),
            Self::Hash(HashClusteringConfig { by, .. }) => by.clone(),
            _ => vec![],
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Range(conf) => conf.multiline_display(),
            Self::Hash(conf) => conf.multiline_display(),
            Self::Random(conf) => conf.multiline_display(),
            Self::Unknown(conf) => conf.multiline_display(),
        }
    }

    pub fn unknown() -> Self {
        Self::Unknown(UnknownClusteringConfig::new(0))
    }

    pub fn unknown_with_num_partitions(num_partitions: usize) -> Self {
        Self::Unknown(UnknownClusteringConfig::new(num_partitions))
    }
}

/// Propagates a clustering spec through a projection.
///
/// Given the clustering of a node's input and the projection applied on top of it, produces the
/// clustering of the projection's output. A clustering key survives if every column it references
/// is still produced by the projection (either passed through, or as part of a larger expression
/// the projection materializes); otherwise the spec downgrades to `Unknown`.
///
/// Operates entirely on **bound** expressions. The `projection` is already bound (that is how the
/// distributed pipeline carries it), and the input clustering keys — which various producers store
/// as resolved-by-name or bound — are normalized to bound columns against `input_schema` up front
/// (binding is idempotent for already-bound columns). Output keys are bound against
/// `output_schema`.
pub fn translate_clustering_spec(
    input_clustering_spec: Arc<ClusteringSpec>,
    projection: &[BoundExpr],
    input_schema: &Schema,
    output_schema: &Schema,
) -> Arc<ClusteringSpec> {
    use crate::partitioning::ClusteringSpec::*;
    let num_partitions = input_clustering_spec.num_partitions();
    let unknown = || ClusteringSpec::Unknown(UnknownClusteringConfig::new(num_partitions)).into();

    match input_clustering_spec.as_ref() {
        // If the scheme is vacuous, the result clustering spec is the same.
        Random(_) | Unknown(_) => input_clustering_spec,
        Range(RangeClusteringConfig { by, .. }) | Hash(HashClusteringConfig { by, .. }) => {
            // Normalize the input clustering keys to bound columns against the input schema.
            // Idempotent for already-bound columns, so this accepts clustering specs from any
            // producer (resolved-by-name from repartition/scan, or already-bound from
            // window/distinct/join).
            let Ok(bound_keys) = BoundExpr::bind_all(by, input_schema) else {
                return unknown();
            };

            // Map each projected expression (outer alias stripped) to the output column it
            // materializes. Both the projection and the bound clustering keys reference the input
            // schema, so a surviving key will structurally match one of these entries.
            let mut projected_to_output: HashMap<ExprRef, ExprRef> = HashMap::new();
            for proj in projection {
                let (inner, _) = proj.inner().unwrap_alias();
                let Ok(out_index) = output_schema.get_index(proj.inner().name()) else {
                    continue;
                };
                let out_col = bound_col(out_index, output_schema[out_index].clone());
                projected_to_output.entry(inner).or_insert(out_col);
            }

            let translated: Option<Vec<ExprRef>> = bound_keys
                .iter()
                .map(|key| translate_bound_clustering_key(key.inner(), &projected_to_output))
                .collect();

            match translated {
                None => unknown(),
                Some(new_by) => match input_clustering_spec.as_ref() {
                    Range(RangeClusteringConfig { descending, .. }) => ClusteringSpec::Range(
                        RangeClusteringConfig::new(num_partitions, new_by, descending.clone()),
                    )
                    .into(),
                    Hash(_) => {
                        ClusteringSpec::Hash(HashClusteringConfig::new(num_partitions, new_by))
                            .into()
                    }
                    _ => unreachable!(),
                },
            }
        }
    }
}

/// Rewrites a bound clustering-key expression so its column references point at the projection's
/// output columns. Returns `None` if the key references an input column that the projection drops
/// (and so cannot be expressed downstream).
fn translate_bound_clustering_key(
    key: &ExprRef,
    projected_to_output: &HashMap<ExprRef, ExprRef>,
) -> Option<ExprRef> {
    let mut dropped_column = false;
    let result = key
        .clone()
        .transform_down(|e| {
            // If this whole sub-expression is materialized as an output column, reference that
            // column and stop descending. Handles both passthrough columns and aliased computed
            // expressions (e.g. `f(col("id")) AS h` lets a key `f(col("id"))` become `col("h")`).
            if let Some(out_col) = projected_to_output.get(&e) {
                return Ok(Transformed::new(
                    out_col.clone(),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            // A bound column not materialized in the output can't be expressed after the projection.
            if matches!(e.as_ref(), Expr::Column(Column::Bound(_))) {
                dropped_column = true;
                return Ok(Transformed::new(e, false, TreeNodeRecursion::Stop));
            }
            Ok(Transformed::no(e))
        })
        .expect("clustering key rewrite is infallible");
    (!dropped_column).then_some(result.data)
}

impl Default for ClusteringSpec {
    fn default() -> Self {
        Self::Unknown(UnknownClusteringConfig::new(1))
    }
}

impl From<RangeClusteringConfig> for ClusteringSpec {
    fn from(value: RangeClusteringConfig) -> Self {
        Self::Range(value)
    }
}

impl From<HashClusteringConfig> for ClusteringSpec {
    fn from(value: HashClusteringConfig) -> Self {
        Self::Hash(value)
    }
}

impl From<RandomClusteringConfig> for ClusteringSpec {
    fn from(value: RandomClusteringConfig) -> Self {
        Self::Random(value)
    }
}

impl From<UnknownClusteringConfig> for ClusteringSpec {
    fn from(value: UnknownClusteringConfig) -> Self {
        Self::Unknown(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RangeClusteringConfig {
    pub num_partitions: usize,
    pub by: Vec<ExprRef>,
    pub descending: Vec<bool>,
}

impl RangeClusteringConfig {
    pub fn new(num_partitions: usize, by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            num_partitions,
            by,
            descending,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        let pairs = self
            .by
            .iter()
            .zip(self.descending.iter())
            .map(|(sb, d)| format!("({}, {})", sb, if *d { "descending" } else { "ascending" },))
            .join(", ");
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!("By = {}", pairs));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct HashClusteringConfig {
    pub num_partitions: usize,
    pub by: Vec<ExprRef>,
}

impl HashClusteringConfig {
    pub fn new(num_partitions: usize, by: Vec<ExprRef>) -> Self {
        Self { num_partitions, by }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Num partitions = {}", self.num_partitions));
        res.push(format!(
            "By = {}",
            self.by.iter().map(|e| e.to_string()).join(", ")
        ));
        res
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct RandomClusteringConfig {
    num_partitions: usize,
}

impl RandomClusteringConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct UnknownClusteringConfig {
    num_partitions: usize,
}

impl UnknownClusteringConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
    }
}

impl Default for UnknownClusteringConfig {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, Operator};
    use daft_dsl::{binary_op, bound_col, lit, resolved_col};
    use daft_schema::{field::Field, schema::Schema};

    use super::*;

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        let fields: Vec<Field> = fields
            .iter()
            .map(|(n, d)| Field::new(*n, d.clone()))
            .collect();
        Schema::new(fields)
    }

    fn field(name: &str, dtype: DataType) -> Field {
        Field::new(name, dtype)
    }

    fn bound(index: usize, name: &str, dtype: DataType) -> ExprRef {
        bound_col(index, field(name, dtype))
    }

    fn bexpr(e: ExprRef) -> BoundExpr {
        BoundExpr::new_unchecked(e)
    }

    fn hash_spec(by: Vec<ExprRef>) -> Arc<ClusteringSpec> {
        ClusteringSpec::Hash(HashClusteringConfig::new(4, by)).into()
    }

    fn hour_of(id: ExprRef) -> ExprRef {
        binary_op(Operator::FloorDivide, id, lit(3_600_000i64))
    }

    #[test]
    fn resolved_keys_passthrough_and_expression_alias() {
        // Input clustered by (producer, hour(id)) declared with resolved-by-name keys (as a
        // repartition or scan source would). A projection passes the columns through and
        // materializes the hour bucket as `h`. The clustering must follow:
        //   Hash([producer, id // 3_600_000]) -> Hash([producer, h]).
        let input_schema = schema(&[
            ("producer", DataType::Utf8),
            ("id", DataType::Int64),
            ("ts", DataType::Int64),
        ]);
        let output_schema = schema(&[
            ("producer", DataType::Utf8),
            ("id", DataType::Int64),
            ("ts", DataType::Int64),
            ("h", DataType::Int64),
        ]);
        let clustering = hash_spec(vec![resolved_col("producer"), hour_of(resolved_col("id"))]);
        let projection = vec![
            bexpr(bound(0, "producer", DataType::Utf8)),
            bexpr(bound(1, "id", DataType::Int64)),
            bexpr(bound(2, "ts", DataType::Int64)),
            bexpr(hour_of(bound(1, "id", DataType::Int64)).alias("h")),
        ];

        let out = translate_clustering_spec(clustering, &projection, &input_schema, &output_schema);
        assert_eq!(
            out.partition_by(),
            vec![
                bound(0, "producer", DataType::Utf8),
                bound(3, "h", DataType::Int64),
            ]
        );
    }

    #[test]
    fn already_bound_keys_are_handled_idempotently() {
        // Input clustering keys are already bound (as window/distinct/join produce them).
        let input_schema = schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let output_schema = schema(&[
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("x", DataType::Int64),
        ]);
        let clustering = hash_spec(vec![
            bound(0, "a", DataType::Int64),
            bound(1, "b", DataType::Int64),
        ]);
        let projection = vec![
            bexpr(bound(0, "a", DataType::Int64)),
            bexpr(bound(1, "b", DataType::Int64)),
            bexpr(binary_op(Operator::Plus, bound(0, "a", DataType::Int64), lit(1i64)).alias("x")),
        ];

        let out = translate_clustering_spec(clustering, &projection, &input_schema, &output_schema);
        assert_eq!(
            out.partition_by(),
            vec![
                bound(0, "a", DataType::Int64),
                bound(1, "b", DataType::Int64)
            ]
        );
    }

    #[test]
    fn expression_unchanged_when_leaf_preserved_without_alias() {
        // The leaf column survives but the expression is not materialized; the key stays an
        // expression, rebased onto the output column.
        let input_schema = schema(&[("id", DataType::Int64), ("other", DataType::Int64)]);
        let output_schema = schema(&[("id", DataType::Int64)]);
        let clustering = hash_spec(vec![hour_of(resolved_col("id"))]);
        let projection = vec![bexpr(bound(0, "id", DataType::Int64))];

        let out = translate_clustering_spec(clustering, &projection, &input_schema, &output_schema);
        assert_eq!(
            out.partition_by(),
            vec![hour_of(bound(0, "id", DataType::Int64))]
        );
    }

    #[test]
    fn downgrades_to_unknown_when_a_referenced_column_is_dropped() {
        // The clustering key references `id`, which the projection drops.
        let input_schema = schema(&[("id", DataType::Int64), ("other", DataType::Int64)]);
        let output_schema = schema(&[("other", DataType::Int64)]);
        let clustering = hash_spec(vec![hour_of(resolved_col("id"))]);
        let projection = vec![bexpr(bound(1, "other", DataType::Int64))];

        let out = translate_clustering_spec(clustering, &projection, &input_schema, &output_schema);
        assert!(matches!(out.as_ref(), ClusteringSpec::Unknown(_)));
    }

    #[test]
    fn unknown_input_is_preserved() {
        let input_schema = schema(&[("a", DataType::Int64)]);
        let output_schema = schema(&[("a", DataType::Int64)]);
        let clustering = ClusteringSpec::unknown_with_num_partitions(8).into();
        let projection = vec![bexpr(bound(0, "a", DataType::Int64))];

        let out = translate_clustering_spec(clustering, &projection, &input_schema, &output_schema);
        assert!(matches!(out.as_ref(), ClusteringSpec::Unknown(_)));
        assert_eq!(out.num_partitions(), 8);
    }
}
