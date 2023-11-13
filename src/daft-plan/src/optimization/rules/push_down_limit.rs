use std::sync::Arc;

use common_error::DaftResult;
use daft_scan::ScanExternalInfo;

use crate::{
    logical_ops::{Limit as LogicalLimit, Source},
    source_info::{ExternalInfo, SourceInfo},
    LogicalPlan,
};

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
        match plan.as_ref() {
            LogicalPlan::Limit(LogicalLimit { input, limit, .. }) => {
                let limit = *limit as usize;
                match input.as_ref() {
                    // Naive commuting with unary ops.
                    //
                    // Limit-UnaryOp -> UnaryOp-Limit
                    LogicalPlan::Repartition(_) | LogicalPlan::Project(_) => {
                        let new_limit = plan.with_new_children(&[input.children()[0].clone()]);
                        Ok(Transformed::Yes(input.with_new_children(&[new_limit])))
                    }
                    // Push limit into source as a "local" limit.
                    //
                    // Limit-Source -> Limit-Source[with_limit]
                    LogicalPlan::Source(source) => {
                        match source.source_info.as_ref() {
                            // Limit pushdown is not supported for in-memory sources.
                            #[cfg(feature = "python")]
                            SourceInfo::InMemoryInfo(_) => Ok(Transformed::No(plan)),
                            // Do not pushdown if Source node is already more limited than `limit`
                            SourceInfo::ExternalInfo(external_info)
                                if let Some(existing_limit) =
                                    external_info.pushdowns().limit && existing_limit <= limit =>
                            {
                                Ok(Transformed::No(plan))
                            }
                            // Pushdown limit into the Source node as a "local" limit
                            SourceInfo::ExternalInfo(external_info) => {
                                let new_pushdowns =
                                    external_info.pushdowns().with_limit(Some(limit));
                                let new_external_info = external_info.with_pushdowns(new_pushdowns);
                                let new_source = LogicalPlan::Source(Source::new(
                                    source.output_schema.clone(),
                                    SourceInfo::ExternalInfo(new_external_info).into(),
                                ))
                                .into();
                                let out_plan =
                                    match external_info {
                                        ExternalInfo::Scan(ScanExternalInfo {
                                            scan_op, ..
                                        }) if scan_op.0.can_absorb_limit() => new_source,
                                        _ => plan.with_new_children(&[new_source]),
                                    };
                                Ok(Transformed::Yes(out_plan))
                            }
                        }
                    }
                    _ => Ok(Transformed::No(plan)),
                }
            }
            _ => Ok(Transformed::No(plan)),
        }
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
        .limit(5, false)?
        .build();
        let expected = "\
        Limit: 5\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Limit pushdown = 5, Output schema = a (Int64), b (Utf8)";
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
        .limit(5, false)?
        .build();
        let expected = "\
        Limit: 5\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Limit pushdown = 3, Output schema = a (Int64), b (Utf8)";
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
        .limit(5, false)?
        .build();
        let expected = "\
        Limit: 5\
        \n  Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Limit pushdown = 5, Output schema = a (Int64), b (Utf8)";
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
            .limit(5, false)?
            .build();
        let expected = "\
        Limit: 5\
        \n . Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Output schema = a (Int64), b (Utf8)";
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
        .repartition(Some(1), vec![col("a")], PartitionScheme::Hash)?
        .limit(5, false)?
        .build();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 1, Partition by = col(a)\
        \n  Limit: 5\
        \n    Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Limit pushdown = 5, Output schema = a (Int64), b (Utf8)";
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
        .limit(5, false)?
        .build();
        let expected = "\
        Project: col(a)\
        \n  Limit: 5\
        \n    Source: Json, File paths = [/foo], File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Storage config = Native(NativeStorageConfig { io_config: None, multithreaded_io: true }), Limit pushdown = 5, Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
