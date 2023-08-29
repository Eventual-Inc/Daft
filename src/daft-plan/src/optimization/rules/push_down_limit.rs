use std::sync::Arc;

use common_error::DaftResult;

use crate::{source_info::SourceInfo, LogicalPlan};

use super::{ApplyOrder, OptimizerRule, Transformed};

/// Optimization rules for pushing Limits further into the logical plan.
#[derive(Default)]
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
    use {
        crate::source_info::{InMemoryInfo, SourceInfo},
        pyo3::Python,
    };

    use crate::{
        ops::{Coalesce, Limit, Project, Repartition, Source},
        optimization::{
            optimizer::{RuleBatch, RuleExecutionStrategy},
            rules::PushDownLimit,
            Optimizer,
        },
        test::dummy_scan_node,
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
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let limit: LogicalPlan = Limit::new(5, source.into()).into();
        let expected = "\
        Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into external Source with existing smaller limit.
    ///
    /// Limit-Source[existing_limit] -> Source[existing_limit]
    #[test]
    fn limit_does_not_push_into_external_source_if_smaller_limit() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .with_limit(Some(3))
        .into();
        let limit: LogicalPlan = Limit::new(5, source.into()).into();
        let expected = "\
        Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), Limit = 3";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into in-memory Source.
    #[test]
    #[cfg(feature = "python")]
    fn limit_does_not_push_into_in_memory_source() -> DaftResult<()> {
        let py_obj = Python::with_gil(|py| py.None());
        let schema: Arc<Schema> = Schema::new(vec![Field::new("a", DataType::Int64)])?.into();
        let source: LogicalPlan = Source::new(
            schema.clone(),
            SourceInfo::InMemoryInfo(InMemoryInfo::new(schema.clone(), "foo".to_string(), py_obj))
                .into(),
            Default::default(),
        )
        .into();
        let limit: LogicalPlan = Limit::new(5, source.into()).into();
        let expected = "\
        Limit: 5\
        \n . Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8)";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Repartition.
    ///
    /// Limit-Repartition-Source -> Repartition-Source[with_limit]
    #[test]
    fn limit_commutes_with_repartition() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let repartition: LogicalPlan =
            Repartition::new(1, vec![col("a")], PartitionScheme::Hash, source.into()).into();
        let limit: LogicalPlan = Limit::new(5, repartition.into()).into();
        let expected = "\
        Repartition: Scheme = Hash, Number of partitions = 1, Partition by = col(a)\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Coalesce.
    ///
    /// Limit-Coalesce-Source -> Coalesce-Source[with_limit]
    #[test]
    fn limit_commutes_with_coalesce() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let coalesce: LogicalPlan = Coalesce::new(1, source.into()).into();
        let limit: LogicalPlan = Limit::new(5, coalesce.into()).into();
        let expected = "\
        Coalesce: To = 1\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Projections.
    ///
    /// Limit-Project-Source -> Project-Source[with_limit]
    #[test]
    fn limit_commutes_with_projection() -> DaftResult<()> {
        let source: LogicalPlan = dummy_scan_node(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ])
        .into();
        let projection: LogicalPlan =
            Project::try_new(source.into(), vec![col("a")], Default::default())?.into();
        let limit: LogicalPlan = Limit::new(5, projection.into()).into();
        let expected = "\
        Project: col(a)\
        \n  Source: \"Json\", File paths = /foo, File schema = a (Int64), b (Utf8), Format-specific config = Json(JsonSourceConfig), Output schema = a (Int64), b (Utf8), Limit = 5";
        assert_optimized_plan_eq(limit.into(), expected)?;
        Ok(())
    }
}
