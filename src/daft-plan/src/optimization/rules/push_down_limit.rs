use std::sync::Arc;

use common_error::DaftResult;

use crate::{source_info::SourceInfo, LogicalPlan};

use super::{ApplyOrder, OptimizerRule, Transformed};

/// Optimization rules for pushing Limits further into the logical plan.
#[derive(Default, Debug)]
pub struct PushDownLimit {}

impl PushDownLimit {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownLimit {
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::TopDown
    }

    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        let limit = match plan.as_ref() {
            LogicalPlan::Limit(limit) => limit,
            _ => return Ok(Transformed::No(plan)),
        };
        let child_plan = limit.input.as_ref();
        let new_plan = match child_plan {
            LogicalPlan::Repartition(_) | LogicalPlan::Coalesce(_) | LogicalPlan::Project(_) => {
                // Naive commuting with unary ops.
                //
                // Limit-UnaryOp -> UnaryOp-Limit
                let new_limit = plan.with_new_children(&[child_plan.children()[0].clone()]);
                child_plan.with_new_children(&[new_limit])
            }
            LogicalPlan::Source(source) => {
                // Push limit into source.
                //
                // Limit-Source -> Source[with_limit]

                // Limit pushdown is only supported for external sources.
                if !matches!(source.source_info.as_ref(), SourceInfo::ExternalInfo(_)) {
                    return Ok(Transformed::No(plan));
                }
                let row_limit = limit.limit as usize;
                // If source already has limit and the existing limit is less than the new limit, unlink the
                // Limit node from the plan and leave the Source node untouched.
                if let Some(existing_source_limit) = source.limit && existing_source_limit <= row_limit {
                    // We directly clone the Limit child rather than creating a new Arc on child_plan to elide
                    // an extra Arc.
                    limit.input.clone()
                } else {
                    // Push limit into Source.
                    let new_source: LogicalPlan = source.with_limit(Some(row_limit)).into();
                    new_source.into()
                }
            }
            _ => return Ok(Transformed::No(plan)),
        };
        Ok(Transformed::Yes(new_plan))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::col;
    use std::sync::Arc;

    #[cfg(feature = "python")]
    use pyo3::Python;

    use crate::{
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownLimit,
            Optimizer,
        },
        test::{dummy_scan_node, dummy_scan_node_with_limit},
        LogicalPlan, PartitionScheme,
    };

    /// Helper that creates an optimizer with the PushDownLimit rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan's repr with
    /// the provided expected repr.
    fn assert_optimized_plan_eq(plan: Arc<LogicalPlan>, expected: &str) -> DaftResult<()> {
        let optimizer = Optimizer::with_rule_batches(
            vec![RuleBatch::new(
                vec![Box::new(PushDownLimit::new())],
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

    /// Tests that Limit pushes into external Source.
    ///
    /// Limit-Source -> Source[with_limit]
    #[test]
    fn limit_pushes_into_external_source() -> DaftResult<()> {
        let plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .limit(5)?
        .build();
        let expected = "\
        Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into external Source with existing smaller limit.
    ///
    /// Limit-Source[existing_limit] -> Source[existing_limit]
    #[test]
    fn limit_does_not_push_into_external_source_if_smaller_limit() -> DaftResult<()> {
        let plan = dummy_scan_node_with_limit(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8),
            ],
            Some(3),
        )
        .limit(5)?
        .build();
        let expected = "\
        Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 3";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does push into external Source with existing larger limit.
    ///
    /// Limit-Source[existing_limit] -> Source[new_limit]
    #[test]
    fn limit_does_push_into_external_source_if_larger_limit() -> DaftResult<()> {
        let plan = dummy_scan_node_with_limit(
            vec![
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::Utf8),
            ],
            Some(10),
        )
        .limit(5)?
        .build();
        let expected = "\
        Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into in-memory Source.
    #[test]
    #[cfg(feature = "python")]
    fn limit_does_not_push_into_in_memory_source() -> DaftResult<()> {
        use crate::LogicalPlanBuilder;

        let py_obj = Python::with_gil(|py| py.None());
        let schema: Arc<Schema> = Schema::new(vec![Field::new("a", DataType::Int64)])?.into();
        let plan = LogicalPlanBuilder::in_memory_scan("foo", py_obj, schema, Default::default())?
            .limit(5)?
            .build();
        let expected = "\
        Limit: 5\
        \n . Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Repartition.
    ///
    /// Limit-Repartition-Source -> Repartition-Source[with_limit]
    #[test]
    fn limit_commutes_with_repartition() -> DaftResult<()> {
        let plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .repartition(1, vec![col("a")], PartitionScheme::Hash)?
        .limit(5)?
        .build();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 1, Partition by = col(a)\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Coalesce.
    ///
    /// Limit-Coalesce-Source -> Coalesce-Source[with_limit]
    #[test]
    fn limit_commutes_with_coalesce() -> DaftResult<()> {
        let plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .coalesce(1)?
        .limit(5)?
        .build();
        let expected = "\
        Coalesce: To = 1\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Projections.
    ///
    /// Limit-Project-Source -> Project-Source[with_limit]
    #[test]
    fn limit_commutes_with_projection() -> DaftResult<()> {
        let plan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .project(vec![col("a")], Default::default())?
        .limit(5)?
        .build();
        let expected = "\
        Project: col(a), Partition spec = PartitionSpec { scheme: Unknown, num_partitions: 1, by: None }\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None }), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
