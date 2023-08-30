use std::sync::Arc;

use common_error::DaftResult;

use crate::{LogicalPlan, PartitionScheme};

use super::{ApplyOrder, OptimizerRule, Transformed};

/// Optimization rules for dropping unnecessary Repartitions.
#[derive(Default)]
pub struct DropRepartition {}

impl DropRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DropRepartition {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let repartition = match plan.as_ref() {
            LogicalPlan::Repartition(repartition) => repartition,
            _ => return Ok(Transformed::No(plan)),
        };
        let child_plan = repartition.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Repartition(_) => {
                // Drop upstream Repartition for back-to-back Repartitions.
                //
                // Repartition1-Repartition2 -> Repartition1
                plan.with_new_children(&[child_plan.children()[0].clone()])
            }
            _ => {
                // Drop a repartition if it would produce the same partition spec as is already produced by the child.
                let parent_partition_spec = plan.partition_spec();
                let child_partition_spec = child_plan.partition_spec();
                if (parent_partition_spec.num_partitions == 1
                    && child_partition_spec.num_partitions == 1)
                    || (child_partition_spec == parent_partition_spec
                        && !matches!(parent_partition_spec.scheme, PartitionScheme::Range))
                {
                    // We directly clone the downstream Repartition child rather than creating a new Arc on child_plan to elide
                    // an extra copy/Arc.
                    repartition.input.clone()
                } else {
                    return Ok(Transformed::No(plan));
                }
            }
        };
        Ok(Transformed::Yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, DataType};
    use daft_dsl::{col, lit, AggExpr};
    use std::sync::Arc;

    use crate::{
        ops::{Aggregate, Filter, Repartition, Sort},
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::drop_repartition::DropRepartition,
            Optimizer,
        },
        test::dummy_scan_node,
        LogicalPlan, PartitionScheme,
    };

    /// Helper that creates an optimizer with the DropRepartition rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan's repr with
    /// the provided expected repr.
    fn assert_optimized_plan_eq(plan: Arc<LogicalPlan>, expected: &str) -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(DropRepartition::new())],
                RuleExecutionStrategy::Once,
            )],
            Default::default(),
        );
        let optimized_plan = optimizer
            .optimize_with_rules(
                optimizer.rule_batches[0].rules.as_slice(),
                plan.clone(),
                &optimizer.rule_batches[0].order,
            )?
            .unwrap()
            .clone();
        assert_eq!(optimized_plan.repr_indent(), expected);

        Ok(())
    }

    /// Tests that DropRepartition does drops the upstream Repartition in back-to-back Repartitions if .
    ///
    /// Repartition1-Repartition2 -> Repartition1
    #[test]
    fn repartition_dropped_in_back_to_back() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let repartition1: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let repartition2: LogicalPlan = Repartition::new(
            5,
            vec![col("a")],
            PartitionScheme::Hash,
            repartition1.into(),
        )
        .into();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 5, Partition by = col(a)\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(repartition2.into(), expected)?;
        Ok(())
    }

    /// Tests that DropRepartition drops a Repartition if both the Repartition and the child have a single partition.
    ///
    /// Repartition-LogicalPlan -> LogicalPlan
    #[test]
    fn repartition_dropped_single_partition() -> DaftResult<()> {
        // dummy_scan_node() will create the default PartitionSpec, which only has a single partition.
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        assert_eq!(source.partition_spec().num_partitions, 1);
        let repartition: LogicalPlan =
            Repartition::new(1, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let expected = "\
        Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(repartition.into(), expected)?;
        Ok(())
    }

    /// Tests that DropRepartition drops a Repartition if both the Repartition and the child have the same partition spec.
    ///
    /// Repartition-LogicalPlan -> LogicalPlan
    #[test]
    fn repartition_dropped_same_partition_spec() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let repartition1: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let filter: LogicalPlan =
            Filter::try_new(col("a").lt(&lit(2)), repartition1.into())?.into();
        let repartition2: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Hash, filter.into()).into();
        let expected = "\
        Filter: col(a) < lit(2)\
        \n  Repartition: Scheme = Hash, Number of partitions = 10, Partition by = col(a)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(repartition2.into(), expected)?;
        Ok(())
    }

    /// Tests that DropRepartition drops a Repartition if both the Repartition and the upstream Aggregation have the same partition spec.
    ///
    /// Repartition-Aggregation -> Aggregation
    #[test]
    fn repartition_dropped_same_partition_spec_agg() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Int64),
        ])
        .into();
        let repartition1: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let agg: LogicalPlan = Aggregate::try_new(
            repartition1.into(),
            vec![AggExpr::Sum(col("a").into())],
            vec![col("b")],
        )?
        .into();
        let repartition2: LogicalPlan =
            Repartition::new(10, vec![col("b")], PartitionScheme::Hash, agg.into()).into();
        let expected = "\
        Aggregation: [Sum(Column(\"a\"))], Group by = [Column(\"b\")], Output schema = b (Int64), a (Int64)\
        \n  Repartition: Scheme = Hash, Number of partitions = 10, Partition by = col(a)\
        \n    Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Int64), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Int64)";
        assert_optimized_plan_eq(repartition2.into(), expected)?;
        Ok(())
    }

    /// Tests that DropRepartition does NOT drop a Repartition if both the Repartition and the child have the same partition spec but they are range-partitioned.
    #[test]
    fn repartition_not_dropped_same_partition_spec() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let repartition1: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let sort: LogicalPlan =
            Sort::try_new(vec![col("a")], vec![true], repartition1.into())?.into();
        let repartition2: LogicalPlan =
            Repartition::new(10, vec![col("a")], PartitionScheme::Range, sort.into()).into();
        let expected = "\
        Repartition: Scheme = Range, Number of partitions = 10, Partition by = col(a)\
        \n  Sort: Sort by = (col(a), descending)\
        \n    Repartition: Scheme = Hash, Number of partitions = 10, Partition by = col(a)\
        \n      Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(repartition2.into(), expected)?;
        Ok(())
    }
}
