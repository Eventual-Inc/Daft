use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    logical_ops::{Limit as LogicalLimit, Source},
    source_info::SourceInfo,
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
            LogicalPlan::Limit(LogicalLimit {
                input,
                limit,
                eager,
            }) => {
                let limit = *limit as usize;
                match input.as_ref() {
                    // Naive commuting with unary ops.
                    //
                    // Limit-UnaryOp -> UnaryOp-Limit
                    LogicalPlan::Repartition(_) | LogicalPlan::Project(_) => {
                        let new_limit = plan
                            .with_new_children(&[input.children()[0].clone()])
                            .into();
                        Ok(Transformed::Yes(
                            input.with_new_children(&[new_limit]).into(),
                        ))
                    }
                    // Push limit into source as a "local" limit.
                    //
                    // Limit-Source -> Limit-Source[with_limit]
                    LogicalPlan::Source(source) => {
                        match source.source_info.as_ref() {
                            // Limit pushdown is not supported for in-memory sources.
                            SourceInfo::InMemory(_) => Ok(Transformed::No(plan)),
                            // Do not pushdown if Source node is already more limited than `limit`
                            SourceInfo::Physical(external_info)
                                if let Some(existing_limit) = external_info.pushdowns.limit
                                    && existing_limit <= limit =>
                            {
                                Ok(Transformed::No(plan))
                            }
                            // Pushdown limit into the Source node as a "local" limit
                            SourceInfo::Physical(external_info) => {
                                let new_pushdowns = external_info.pushdowns.with_limit(Some(limit));
                                let new_external_info = external_info.with_pushdowns(new_pushdowns);
                                let new_source = LogicalPlan::Source(Source::new(
                                    source.output_schema.clone(),
                                    SourceInfo::Physical(new_external_info).into(),
                                ))
                                .into();
                                let out_plan = if external_info.scan_op.0.can_absorb_limit() {
                                    new_source
                                } else {
                                    plan.with_new_children(&[new_source]).into()
                                };
                                Ok(Transformed::Yes(out_plan))
                            }
                            SourceInfo::PlaceHolder(..) => {
                                panic!("PlaceHolderInfo should not exist for optimization!");
                            }
                        }
                    }
                    // Fold Limit together.
                    //
                    // Limit-Limit -> Limit
                    LogicalPlan::Limit(LogicalLimit {
                        input,
                        limit: child_limit,
                        eager: child_eagar,
                    }) => {
                        let new_limit = limit.min(*child_limit as usize);
                        let new_eager = eager | child_eagar;

                        let new_plan = Arc::new(LogicalPlan::Limit(LogicalLimit::new(
                            input.clone(),
                            new_limit as i64,
                            new_eager,
                        )));
                        // we rerun the optimizer, ideally when we move to a visitor pattern this should go away
                        let optimized = self
                            .try_optimize(new_plan.clone())?
                            .or(Transformed::Yes(new_plan))
                            .unwrap()
                            .clone();
                        Ok(Transformed::Yes(optimized))
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
    use daft_scan::Pushdowns;
    use rstest::rstest;
    use std::sync::Arc;

    #[cfg(feature = "python")]
    use pyo3::Python;

    use crate::{
        logical_optimization::{rules::PushDownLimit, test::assert_optimized_plan_with_rules_eq},
        test::{dummy_scan_node, dummy_scan_node_with_pushdowns, dummy_scan_operator},
        LogicalPlan, LogicalPlanBuilder,
    };

    /// Helper that creates an optimizer with the PushDownLimit rule registered, optimizes
    /// the provided plan with said optimizer, and compares the optimized plan with
    /// the provided expected plan.
    fn assert_optimized_plan_eq(
        plan: Arc<LogicalPlan>,
        expected: Arc<LogicalPlan>,
    ) -> DaftResult<()> {
        assert_optimized_plan_with_rules_eq(plan, expected, vec![Box::new(PushDownLimit::new())])
    }

    /// Tests that Limit pushes into external Source.
    ///
    /// Limit-Source -> Source[with_limit]
    #[test]
    fn limit_pushes_into_external_source() -> DaftResult<()> {
        let limit = 5;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into scan with existing smaller limit.
    ///
    /// Limit-Source[existing_limit] -> Source[existing_limit]
    #[test]
    fn limit_does_not_push_into_scan_if_smaller_limit() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 3;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does push into scan with existing larger limit.
    ///
    /// Limit-Source[existing_limit] -> Source[new_limit]
    #[test]
    fn limit_does_push_into_scan_if_larger_limit() -> DaftResult<()> {
        let limit = 5;
        let existing_limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node_with_pushdowns(
            scan_op.clone(),
            Pushdowns::default().with_limit(Some(existing_limit)),
        )
        .limit(limit, false)?
        .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that multiple adjacent Limits fold into the smallest limit.
    ///
    /// Limit[x]-Limit[y] -> Limit[min(x,y)]
    #[rstest]
    fn limit_folds_with_smaller_limit(
        #[values(false, true)] smaller_first: bool,
    ) -> DaftResult<()> {
        let smaller_limit = 5;
        let limit = 10;
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .limit(if smaller_first { smaller_limit } else { limit }, false)?
            .limit(if smaller_first { limit } else { smaller_limit }, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(smaller_limit as usize)),
        )
        .limit(smaller_limit, false)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit does not push into in-memory Source.
    #[test]
    #[cfg(feature = "python")]
    fn limit_does_not_push_into_in_memory_source() -> DaftResult<()> {
        let py_obj = Python::with_gil(|py| py.None());
        let schema: Arc<Schema> = Schema::new(vec![Field::new("a", DataType::Int64)])?.into();
        let plan =
            LogicalPlanBuilder::in_memory_scan("foo", py_obj, schema, Default::default(), 5, 3)?
                .limit(5, false)?
                .build();
        assert_optimized_plan_eq(plan.clone(), plan)?;
        Ok(())
    }

    /// Tests that Limit commutes with Repartition.
    ///
    /// Limit-Repartition-Source -> Repartition-Source[with_limit]
    #[test]
    fn limit_commutes_with_repartition() -> DaftResult<()> {
        let limit = 5;
        let num_partitions = 1;
        let partition_by = vec![col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .hash_repartition(Some(num_partitions), partition_by.clone())?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .hash_repartition(Some(num_partitions), partition_by)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }

    /// Tests that Limit commutes with Projections.
    ///
    /// Limit-Project-Source -> Project-Source[with_limit]
    #[test]
    fn limit_commutes_with_projection() -> DaftResult<()> {
        let limit = 5;
        let proj = vec![col("a")];
        let scan_op = dummy_scan_operator(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Utf8),
        ]);
        let plan = dummy_scan_node(scan_op.clone())
            .select(proj.clone())?
            .limit(limit, false)?
            .build();
        let expected = dummy_scan_node_with_pushdowns(
            scan_op,
            Pushdowns::default().with_limit(Some(limit as usize)),
        )
        .limit(limit, false)?
        .select(proj)?
        .build();
        assert_optimized_plan_eq(plan, expected)?;
        Ok(())
    }
}
