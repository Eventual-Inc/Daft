use std::sync::Arc;

use daft_dsl::ExprRef;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

/// Repartitioning specification.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RepartitionSpec {
    Hash(HashRepartitionConfig),
    Random(RandomShuffleConfig),
    IntoPartitions(IntoPartitionsConfig),
}

impl RepartitionSpec {
    pub fn var_name(&self) -> &'static str {
        match self {
            Self::Hash(_) => "Hash",
            Self::Random(_) => "Random",
            Self::IntoPartitions(_) => "IntoPartitions",
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
            Self::IntoPartitions(conf) => conf.multiline_display(),
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
            Self::Random(RandomShuffleConfig { num_partitions }) => ClusteringSpec::Random(
                RandomClusteringConfig::new(num_partitions.unwrap_or(upstream_num_partitions)),
            ),
            Self::IntoPartitions(IntoPartitionsConfig { num_partitions }) => {
                ClusteringSpec::Unknown(UnknownClusteringConfig::new(*num_partitions))
            }
        }
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
}

impl RandomShuffleConfig {
    pub fn new(num_partitions: Option<usize>) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {:?}", self.num_partitions)]
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct IntoPartitionsConfig {
    pub num_partitions: usize,
}

impl IntoPartitionsConfig {
    pub fn new(num_partitions: usize) -> Self {
        Self { num_partitions }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![format!("Num partitions = {}", self.num_partitions)]
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
}

pub fn translate_clustering_spec(
    input_clustering_spec: Arc<ClusteringSpec>,
    projection: &Vec<ExprRef>,
) -> Arc<ClusteringSpec> {
    // Given an input clustering spec, and a new projection,
    // produce the new clustering spec.

    use crate::partitioning::ClusteringSpec::*;
    match input_clustering_spec.as_ref() {
        // If the scheme is vacuous, the result partition spec is the same.
        Random(_) | Unknown(_) => input_clustering_spec,
        // Otherwise, need to reevaluate the partition scheme for each expression.
        Range(RangeClusteringConfig { by, .. }) | Hash(HashClusteringConfig { by, .. }) => {
            // See what columns the projection directly translates into new columns.
            let mut old_colname_to_new_colname = IndexMap::new();
            for expr in projection {
                if let Some(oldname) = expr.input_mapping() {
                    let newname = expr.name().to_string();
                    // Add the oldname -> newname mapping,
                    // but don't overwrite any existing identity mappings (e.g. "a" -> "a").
                    if old_colname_to_new_colname.get(&oldname) != Some(&oldname) {
                        old_colname_to_new_colname.insert(oldname, newname);
                    }
                }
            }

            // Then, see if we can fully translate the clustering spec.
            let maybe_new_clustering_spec = by
                .iter()
                .map(|e| translate_clustering_spec_expr(e, &old_colname_to_new_colname))
                .collect::<std::result::Result<Vec<_>, _>>();
            maybe_new_clustering_spec.map_or_else(
                |()| {
                    ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                        input_clustering_spec.num_partitions(),
                    ))
                    .into()
                },
                |new_clustering_spec: Vec<ExprRef>| match input_clustering_spec.as_ref() {
                    Range(RangeClusteringConfig {
                        num_partitions,
                        descending,
                        ..
                    }) => ClusteringSpec::Range(RangeClusteringConfig::new(
                        *num_partitions,
                        new_clustering_spec,
                        descending.clone(),
                    ))
                    .into(),
                    Hash(HashClusteringConfig { num_partitions, .. }) => ClusteringSpec::Hash(
                        HashClusteringConfig::new(*num_partitions, new_clustering_spec),
                    )
                    .into(),
                    _ => unreachable!(),
                },
            )
        }
    }
}

fn translate_clustering_spec_expr(
    clustering_spec_expr: &ExprRef,
    old_colname_to_new_colname: &IndexMap<String, String>,
) -> std::result::Result<ExprRef, ()> {
    // Given a single expression of an input clustering spec,
    // translate it to a new expression in the given projection.
    // Returns:
    //  - Ok(expr) with expr being the translation, or
    //  - Err(()) if no translation is possible in the new projection.

    use daft_dsl::{binary_op, Expr};

    match clustering_spec_expr.as_ref() {
        Expr::Column(name) => match old_colname_to_new_colname.get(name.as_ref()) {
            Some(newname) => Ok(daft_dsl::col(newname.as_str())),
            None => Err(()),
        },
        Expr::Literal(_) => Ok(clustering_spec_expr.clone()),
        Expr::Alias(child, name) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            Ok(newchild.alias(name.clone()))
        }
        Expr::BinaryOp { op, left, right } => {
            let newleft = translate_clustering_spec_expr(left, old_colname_to_new_colname)?;
            let newright = translate_clustering_spec_expr(right, old_colname_to_new_colname)?;
            Ok(binary_op(*op, newleft, newright))
        }
        Expr::Cast(child, dtype) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            Ok(newchild.cast(dtype))
        }
        Expr::Function { func, inputs } => {
            let new_inputs = inputs
                .iter()
                .map(|e| translate_clustering_spec_expr(e, old_colname_to_new_colname))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Function {
                func: func.clone(),
                inputs: new_inputs,
            }
            .into())
        }
        Expr::ScalarFunction(func) => {
            let mut func = func.clone();
            let new_inputs = func
                .inputs
                .iter()
                .map(|e| translate_clustering_spec_expr(e, old_colname_to_new_colname))
                .collect::<Result<Vec<_>, _>>()?;
            func.inputs = new_inputs;
            Ok(Expr::ScalarFunction(func).into())
        }
        Expr::Not(child) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            Ok(newchild.not())
        }
        Expr::IsNull(child) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            Ok(newchild.is_null())
        }
        Expr::NotNull(child) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            Ok(newchild.not_null())
        }
        Expr::FillNull(child, fill_value) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            let newfill = translate_clustering_spec_expr(fill_value, old_colname_to_new_colname)?;
            Ok(newchild.fill_null(newfill))
        }
        Expr::IsIn(child, items) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            let newitems = translate_clustering_spec_expr(items, old_colname_to_new_colname)?;
            Ok(newchild.is_in(newitems))
        }
        Expr::Between(child, lower, upper) => {
            let newchild = translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
            let newlower = translate_clustering_spec_expr(lower, old_colname_to_new_colname)?;
            let newupper = translate_clustering_spec_expr(upper, old_colname_to_new_colname)?;
            Ok(newchild.between(newlower, newupper))
        }
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => {
            let newtrue = translate_clustering_spec_expr(if_true, old_colname_to_new_colname)?;
            let newfalse = translate_clustering_spec_expr(if_false, old_colname_to_new_colname)?;
            let newpred = translate_clustering_spec_expr(predicate, old_colname_to_new_colname)?;

            Ok(newpred.if_else(newtrue, newfalse))
        }
        // Cannot have agg exprs in clustering specs.
        Expr::Agg(_) => Err(()),
    }
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
        UnknownClusteringConfig::new(1)
    }
}
