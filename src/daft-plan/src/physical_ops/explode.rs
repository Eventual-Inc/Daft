use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{optimization::get_required_columns, ExprRef};
use itertools::Itertools;

use crate::{
    partitioning::{HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig},
    physical_plan::PhysicalPlanRef,
    ClusteringSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Explode {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub to_explode: Vec<ExprRef>,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl Explode {
    pub(crate) fn try_new(input: PhysicalPlanRef, to_explode: Vec<ExprRef>) -> DaftResult<Self> {
        let clustering_spec = Self::translate_clustering_spec(input.clustering_spec(), &to_explode);
        Ok(Self {
            input,
            to_explode,
            clustering_spec,
        })
    }

    fn translate_clustering_spec(
        input_clustering_spec: Arc<ClusteringSpec>,
        to_explode: &Vec<ExprRef>,
    ) -> Arc<ClusteringSpec> {
        use crate::ClusteringSpec::*;
        match input_clustering_spec.as_ref() {
            // If the scheme is vacuous, the result partition spec is the same.
            Random(_) | Unknown(_) => input_clustering_spec,
            // Otherwise, need to reevaluate the partition scheme for each expression.
            Range(RangeClusteringConfig { by, .. }) | Hash(HashClusteringConfig { by, .. }) => {
                let required_cols_for_clustering_spec = by
                    .iter()
                    .flat_map(get_required_columns)
                    .collect::<HashSet<String>>();
                for expr in to_explode {
                    let newname = expr.name().to_string();
                    // if we clobber one of the required columns for the clustering_spec, invalidate it.
                    if required_cols_for_clustering_spec.contains(&newname) {
                        return ClusteringSpec::Unknown(UnknownClusteringConfig::new(
                            input_clustering_spec.num_partitions(),
                        ))
                        .into();
                    }
                }
                input_clustering_spec
            }
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Explode: {}",
            self.to_explode.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::col;

    use crate::{
        partitioning::{HashClusteringConfig, UnknownClusteringConfig},
        physical_planner::logical_to_physical,
        test::{dummy_scan_node, dummy_scan_operator},
        ClusteringSpec,
    };

    /// do not destroy the partition spec.
    #[test]
    fn test_clustering_spec_preserving() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();

        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::List(Box::new(DataType::Int64))),
            Field::new("c", DataType::Int64),
        ]))
        .hash_repartition(Some(3), vec![col("a")])?
        .explode(vec![col("b")])?
        .build();

        let physical_plan = logical_to_physical(logical_plan, cfg)?;

        let expected_clustering_spec =
            ClusteringSpec::Hash(HashClusteringConfig::new(3, vec![col("a")]));

        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }

    /// do not destroy the partition spec.
    #[test]
    fn test_clustering_spec_destroying() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();

        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::List(Box::new(DataType::Int64))),
            Field::new("c", DataType::Int64),
        ]))
        .hash_repartition(Some(3), vec![col("a"), col("b")])?
        .explode(vec![col("b")])?
        .build();

        let physical_plan = logical_to_physical(logical_plan, cfg)?;

        let expected_clustering_spec = ClusteringSpec::Unknown(UnknownClusteringConfig::new(3));

        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }
}
