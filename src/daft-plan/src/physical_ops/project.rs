use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{binary_op, Expr, ExprRef};
use indexmap::IndexMap;
use itertools::Itertools;

use crate::{
    partitioning::{HashClusteringConfig, RangeClusteringConfig, UnknownClusteringConfig},
    physical_plan::PhysicalPlanRef,
    ClusteringSpec, ResourceRequest,
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
        let clustering_spec = Self::translate_clustering_spec(clustering_spec, &projection);
        Ok(Self {
            input,
            projection,
            resource_request,
            clustering_spec,
        })
    }

    fn translate_clustering_spec(
        input_clustering_spec: Arc<ClusteringSpec>,
        projection: &Vec<ExprRef>,
    ) -> Arc<ClusteringSpec> {
        // Given an input partition spec, and a new projection,
        // produce the new partition spec.

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
                        let newname = expr.name().unwrap().to_string();
                        // Add the oldname -> newname mapping,
                        // but don't overwrite any existing identity mappings (e.g. "a" -> "a").
                        if old_colname_to_new_colname.get(&oldname) != Some(&oldname) {
                            old_colname_to_new_colname.insert(oldname, newname);
                        }
                    }
                }

                // Then, see if we can fully translate the partition spec.
                let maybe_new_clustering_spec = by
                    .iter()
                    .map(|e| Self::translate_clustering_spec_expr(e, &old_colname_to_new_colname))
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
        // Given a single expression of an input partition spec,
        // translate it to a new expression in the given projection.
        // Returns:
        //  - Ok(expr) with expr being the translation, or
        //  - Err(()) if no translation is possible in the new projection.

        match clustering_spec_expr.as_ref() {
            Expr::Column(name) => match old_colname_to_new_colname.get(name.as_ref()) {
                Some(newname) => Ok(daft_dsl::col(newname.as_str())),
                None => Err(()),
            },
            Expr::Literal(_) => Ok(clustering_spec_expr.clone()),
            Expr::Alias(child, name) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                Ok(newchild.alias(name.clone()))
            }
            Expr::BinaryOp { op, left, right } => {
                let newleft =
                    Self::translate_clustering_spec_expr(left, old_colname_to_new_colname)?;
                let newright =
                    Self::translate_clustering_spec_expr(right, old_colname_to_new_colname)?;
                Ok(binary_op(*op, newleft, newright))
            }
            Expr::Cast(child, dtype) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                Ok(newchild.cast(dtype))
            }
            Expr::Function { func, inputs } => {
                let new_inputs = inputs
                    .iter()
                    .map(|e| Self::translate_clustering_spec_expr(e, old_colname_to_new_colname))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expr::Function {
                    func: func.clone(),
                    inputs: new_inputs,
                }
                .into())
            }
            Expr::Not(child) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                Ok(newchild.not())
            }
            Expr::IsNull(child) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                Ok(newchild.is_null())
            }
            Expr::NotNull(child) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                Ok(newchild.not_null())
            }
            Expr::FillNull(child, fill_value) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                let newfill =
                    Self::translate_clustering_spec_expr(fill_value, old_colname_to_new_colname)?;
                Ok(newchild.fill_null(newfill))
            }
            Expr::IsIn(child, items) => {
                let newchild =
                    Self::translate_clustering_spec_expr(child, old_colname_to_new_colname)?;
                let newitems =
                    Self::translate_clustering_spec_expr(items, old_colname_to_new_colname)?;
                Ok(newchild.is_in(newitems))
            }
            Expr::IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let newtrue =
                    Self::translate_clustering_spec_expr(if_true, old_colname_to_new_colname)?;
                let newfalse =
                    Self::translate_clustering_spec_expr(if_false, old_colname_to_new_colname)?;
                let newpred =
                    Self::translate_clustering_spec_expr(predicate, old_colname_to_new_colname)?;

                Ok(newpred.if_else(newtrue, newfalse))
            }
            // Cannot have agg exprs in partition specs.
            Expr::Agg(_) => Err(()),
        }
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
        physical_planner::plan,
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
        .project(expressions, Default::default())?
        .build();

        let physical_plan = plan(logical_plan, cfg)?;

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
        .project(projection, Default::default())?
        .build();

        let physical_plan = plan(logical_plan, cfg)?;

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
        .project(expressions, Default::default())?
        .build();

        let physical_plan = plan(logical_plan, cfg)?;

        let expected_clustering_spec =
            ClusteringSpec::Hash(HashClusteringConfig::new(3, vec![col("a"), col("b")]));

        assert_eq!(
            expected_clustering_spec,
            physical_plan.clustering_spec().as_ref().clone()
        );

        Ok(())
    }
}
