use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{optimization::get_required_columns, Expr};
use itertools::Itertools;

use crate::{physical_plan::PhysicalPlanRef, PartitionSpec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Explode {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub to_explode: Vec<Expr>,
    pub partition_spec: Arc<PartitionSpec>,
}

impl Explode {
    pub(crate) fn try_new(input: PhysicalPlanRef, to_explode: Vec<Expr>) -> DaftResult<Self> {
        let partition_spec = Self::translate_partition_spec(input.partition_spec(), &to_explode);
        Ok(Self {
            input,
            to_explode,
            partition_spec,
        })
    }

    fn translate_partition_spec(
        input_pspec: Arc<PartitionSpec>,
        to_explode: &Vec<Expr>,
    ) -> Arc<PartitionSpec> {
        use crate::PartitionSchemeConfig::*;
        match input_pspec.scheme_config {
            // If the scheme is vacuous, the result partiiton spec is the same.
            Random(_) | Unknown(_) => input_pspec.clone(),
            // Otherwise, need to reevaluate the partition scheme for each expression.
            Range(_) | Hash(_) => {
                let required_cols_for_pspec = input_pspec
                    .by
                    .as_ref()
                    .map(|b| {
                        b.iter()
                            .flat_map(get_required_columns)
                            .collect::<HashSet<String>>()
                    })
                    .expect("Range or Hash partitioned PSpec should be partitioned by something");
                for expr in to_explode {
                    let newname = expr.name().unwrap().to_string();
                    // if we clobber one of the required columns for the pspec, invalidate it.
                    if required_cols_for_pspec.contains(&newname) {
                        return PartitionSpec::new(
                            Unknown(Default::default()),
                            input_pspec.num_partitions,
                            None,
                        )
                        .into();
                    }
                }
                input_pspec
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
            "Partition spec = {{ {} }}",
            self.partition_spec.multiline_display().join(", ")
        ));
        res
    }
}

#[cfg(test)]
mod tests {
    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, Expr};

    use crate::{planner::plan, test::dummy_scan_node, PartitionSchemeConfig, PartitionSpec};

    /// do not destroy the partition spec.
    #[test]
    fn test_partition_spec_preserving() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();

        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::List(Box::new(DataType::Int64))),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            Some(3),
            vec![Expr::Column("a".into())],
            PartitionSchemeConfig::Hash(Default::default()),
        )?
        .explode(vec![col("b")])?
        .build();

        let physical_plan = plan(&logical_plan, cfg)?;

        let expected_pspec = PartitionSpec::new(
            PartitionSchemeConfig::Hash(Default::default()),
            3,
            Some(vec![col("a")]),
        );

        assert_eq!(
            expected_pspec,
            physical_plan.partition_spec().as_ref().clone()
        );

        Ok(())
    }

    /// do not destroy the partition spec.
    #[test]
    fn test_partition_spec_destroying() -> DaftResult<()> {
        let cfg = DaftExecutionConfig::default().into();

        let logical_plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::List(Box::new(DataType::Int64))),
            Field::new("c", DataType::Int64),
        ])
        .repartition(
            Some(3),
            vec![Expr::Column("a".into()), Expr::Column("b".into())],
            PartitionSchemeConfig::Hash(Default::default()),
        )?
        .explode(vec![col("b")])?
        .build();

        let physical_plan = plan(&logical_plan, cfg)?;

        let expected_pspec =
            PartitionSpec::new(PartitionSchemeConfig::Unknown(Default::default()), 3, None);

        assert_eq!(
            expected_pspec,
            physical_plan.partition_spec().as_ref().clone()
        );

        Ok(())
    }
}
