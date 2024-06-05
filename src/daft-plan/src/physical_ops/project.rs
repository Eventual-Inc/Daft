use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use itertools::Itertools;

use crate::{
    partitioning::translate_clustering_spec, physical_plan::PhysicalPlanRef, ClusteringSpec,
    ResourceRequest,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Project {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub projection: Vec<ExprRef>,
    pub resource_request: ResourceRequest,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl Project {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        projection: Vec<ExprRef>,
        resource_request: ResourceRequest,
        clustering_spec: Arc<ClusteringSpec>,
    ) -> DaftResult<Self> {
        let clustering_spec = translate_clustering_spec(clustering_spec, &projection);
        Ok(Self {
            input,
            projection,
            resource_request,
            clustering_spec,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Project: {}",
            self.projection.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        let resource_request = self.resource_request.multiline_display();
        if !resource_request.is_empty() {
            res.push(format!(
                "Resource request = {{ {} }}",
                resource_request.join(", ")
            ));
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit, ExprRef};
    use rstest::rstest;

    use crate::{
        partitioning::{ClusteringSpec, HashClusteringConfig, UnknownClusteringConfig},
        physical_planner::logical_to_physical,
        test::{dummy_scan_node, dummy_scan_operator},
    };

    /// Test that projections preserving column inputs, even through aliasing,
    /// do not destroy the partition spec.
    #[test]
    fn test_clustering_spec_preserving() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let expressions = vec![
            (col("a").rem(lit(2))), // this is now "a"
            col("b"),
            col("a").alias("aa"),
        ];
        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]))
        .hash_repartition(Some(3), vec![col("a"), col("b")])?
        .select(expressions)?
        .build();

        let physical_plan = logical_to_physical(logical_plan, cfg)?;

        let expected_clustering_spec =
            ClusteringSpec::Hash(HashClusteringConfig::new(3, vec![col("aa"), col("b")]));

        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }

    /// Test that projections destroying even a single column input from the partition spec
    /// destroys the entire partition spec.
    #[rstest]
    fn test_clustering_spec_destroying(
        #[values(
            vec![col("a"), col("c").alias("b")], // original "b" is gone even though "b" is present
            vec![col("b")],                      // original "a" dropped
            vec![col("a").rem(lit(2)), col("b")],   // original "a" gone
            vec![col("c")],                      // everything gone
        )]
        projection: Vec<ExprRef>,
    ) -> DaftResult<()> {
        use crate::partitioning::ClusteringSpec;

        let cfg = DaftExecutionConfig::default().into();
        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]))
        .hash_repartition(Some(3), vec![col("a"), col("b")])?
        .select(projection)?
        .build();

        let physical_plan = logical_to_physical(logical_plan, cfg)?;

        let expected_clustering_spec = ClusteringSpec::Unknown(UnknownClusteringConfig::new(3));
        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }

    /// Test that new partition specs favor existing instead of new names.
    /// i.e. ("a", "a" as "b") remains partitioned by "a", not "b"
    #[test]
    fn test_clustering_spec_prefer_existing_names() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();
        let expressions = vec![col("a").alias("y"), col("a"), col("a").alias("z"), col("b")];

        let logical_plan = dummy_scan_node(dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
            Field::new("c", DataType::Int64),
        ]))
        .hash_repartition(Some(3), vec![col("a"), col("b")])?
        .select(expressions)?
        .build();

        let physical_plan = logical_to_physical(logical_plan, cfg)?;

        let expected_clustering_spec =
            ClusteringSpec::Hash(HashClusteringConfig::new(3, vec![col("a"), col("b")]));

        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }
}
