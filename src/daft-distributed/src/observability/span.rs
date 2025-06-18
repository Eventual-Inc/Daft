use std::sync::Arc;

use crate::{
    observability::{HooksManager, PlanEvent},
    pipeline_node::DistributedPipelineNode,
    plan::DistributedPhysicalPlan,
    stage::Stage,
};

/// A `span` that represents the lifetime of a `Plan`.
pub(crate) struct PlanSpan<'a> {
    plan: &'a DistributedPhysicalPlan,
    plan_id: &'a str,
    hooks_manager: &'a HooksManager,
}

impl<'a> PlanSpan<'a> {
    pub(crate) fn new(
        plan: &'a DistributedPhysicalPlan,
        plan_id: &'a str,
        hooks_manager: &'a HooksManager,
    ) -> Self {
        hooks_manager.emit(&PlanEvent::PlanStarted { plan, plan_id });
        Self {
            plan,
            plan_id,
            hooks_manager,
        }
    }
}

impl Drop for PlanSpan<'_> {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::PlanCompleted {
            plan: self.plan,
            plan_id: self.plan_id,
        });
    }
}

/// A `span` that represents the lifetime of a `Stage`.
pub(crate) struct StageSpan<'a> {
    stage: &'a Stage,
    hooks_manager: &'a HooksManager,
    plan_id: &'a str,
}

impl<'a> StageSpan<'a> {
    pub(crate) fn new(stage: &'a Stage, plan_id: &'a str, hooks_manager: &'a HooksManager) -> Self {
        hooks_manager.emit(&PlanEvent::StageStarted { stage, plan_id });
        Self {
            stage,
            hooks_manager,
            plan_id,
        }
    }
    pub(crate) fn new_pipeline_span(
        &self,
        pipeline_node: Arc<dyn DistributedPipelineNode>,
    ) -> PipelineNodeSpan {
        PipelineNodeSpan::new(pipeline_node, self.hooks_manager.clone())
    }
}

impl Drop for StageSpan<'_> {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::StageCompleted {
            stage: self.stage,
            plan_id: self.plan_id,
        });
    }
}

/// A `span` that represents the lifetime of a `PipelineNode`.
/// Unlike the other spans, `PipelineNodeSpan` requires an owned context due to async lifetimes
pub(crate) struct PipelineNodeSpan {
    pipeline_node: Arc<dyn DistributedPipelineNode>,
    hooks_manager: HooksManager,
}

impl PipelineNodeSpan {
    pub(crate) fn new(
        pipeline_node: Arc<dyn DistributedPipelineNode>,
        hooks_manager: HooksManager,
    ) -> Self {
        hooks_manager.emit(&PlanEvent::PipelineNodeStarted {
            pipeline_node: &pipeline_node,
        });
        Self {
            pipeline_node,
            hooks_manager,
        }
    }
}

impl Drop for PipelineNodeSpan {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::PipelineNodeCompleted {
            pipeline_node: &self.pipeline_node,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_daft_config::DaftExecutionConfig;
    use common_error::DaftResult;
    use common_scan_info::{test::DummyScanOperator, Pushdowns, ScanOperatorRef};
    use daft_logical_plan::LogicalPlanBuilder;
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;

    /// Create a dummy scan node containing the provided fields in its schema and the provided limit.
    pub fn dummy_scan_operator(fields: Vec<Field>) -> ScanOperatorRef {
        let schema = Arc::new(Schema::new(fields));
        ScanOperatorRef(Arc::new(DummyScanOperator {
            schema,
            num_scan_tasks: 1,
            num_rows_per_task: None,
        }))
    }

    /// Create a dummy scan node containing the provided fields in its schema.
    pub fn dummy_scan_node(scan_op: ScanOperatorRef) -> LogicalPlanBuilder {
        dummy_scan_node_with_pushdowns(scan_op, Default::default())
    }

    /// Create a dummy scan node containing the provided fields in its schema and the provided limit.
    pub fn dummy_scan_node_with_pushdowns(
        scan_op: ScanOperatorRef,
        pushdowns: Pushdowns,
    ) -> LogicalPlanBuilder {
        LogicalPlanBuilder::table_scan(scan_op, Some(pushdowns)).unwrap()
    }

    use crate::{observability::PlanObserver, pipeline_node::NodeID, plan::PlanID, stage::StageID};

    #[derive(Clone)]
    struct TestObserver {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl TestObserver {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl PlanObserver for TestObserver {
        fn on_event(&self, event: &PlanEvent) -> DaftResult<()> {
            self.events.lock().unwrap().push(match event {
                PlanEvent::PlanStarted { plan_id, .. } => format!("PlanStarted({})", plan_id),
                PlanEvent::PlanCompleted { plan_id, .. } => format!("PlanCompleted({})", plan_id),
                PlanEvent::StageStarted { stage, plan_id } => {
                    format!("StageStarted({}, {})", plan_id, stage.id)
                }
                PlanEvent::StageCompleted { stage, plan_id } => {
                    format!("StageCompleted({}, {})", plan_id, stage.id)
                }
                PlanEvent::PipelineNodeStarted { pipeline_node } => {
                    format!(
                        "PipelineNodeStarted({}, {}, {})",
                        pipeline_node.plan_id(),
                        pipeline_node.stage_id(),
                        pipeline_node.node_id()
                    )
                }
                PlanEvent::PipelineNodeCompleted { pipeline_node } => {
                    format!(
                        "PipelineNodeCompleted({}, {}, {})",
                        pipeline_node.plan_id(),
                        pipeline_node.stage_id(),
                        pipeline_node.node_id()
                    )
                }
            });
            Ok(())
        }
    }

    fn create_test_hooks_manager() -> (HooksManager, TestObserver) {
        let observer = TestObserver::new();
        let hooks_manager = HooksManager {
            subscribers: vec![Arc::new(observer.clone())],
        };
        (hooks_manager, observer)
    }

    #[test]
    fn test_plan_span_events() {
        let (hooks_manager, observer) = create_test_hooks_manager();
        let plan = create_mock_plan();
        let plan_id = "test_plan_123";

        {
            let _span = PlanSpan::new(&plan, plan_id, &hooks_manager);
            let events = observer.get_events();
            assert_eq!(events.len(), 1);
            let expected = "PlanStarted(test_plan_123)";
            assert_eq!(events[0], expected);
        }

        let events = observer.get_events();
        assert_eq!(events.len(), 2);
        let expected = "PlanCompleted(test_plan_123)";
        assert_eq!(events[1], expected);
    }

    #[test]
    fn test_stage_span_events() {
        let (hooks_manager, observer) = create_test_hooks_manager();
        let stage = create_mock_stage();
        let plan_id = "test_plan_456";

        {
            let _span = StageSpan::new(&stage, plan_id, &hooks_manager);
            let events = observer.get_events();
            assert_eq!(events.len(), 1);
            let expected = "StageStarted(test_plan_456, 0)";
            assert_eq!(events[0], expected);
        }

        let events = observer.get_events();
        assert_eq!(events.len(), 2);
        let expected = "StageCompleted(test_plan_456, 0)";
        assert_eq!(events[1], expected);
    }

    #[test]
    fn test_pipeline_node_span_events() {
        let (hooks_manager, observer) = create_test_hooks_manager();
        let pipeline_node = create_mock_pipeline_node(1, 1, "test_plan_456");

        {
            let _span = PipelineNodeSpan::new(pipeline_node.clone(), hooks_manager);
            let events = observer.get_events();
            assert_eq!(events.len(), 1);
            let expected = "PipelineNodeStarted(test_plan_456, 1, 1)";
            assert_eq!(events[0], expected);
        }

        let events = observer.get_events();
        assert_eq!(events.len(), 2);
        let expected = "PipelineNodeCompleted(test_plan_456, 1, 1)";
        assert_eq!(events[1], expected);
    }

    #[test]
    fn test_nested_spans() {
        let (hooks_manager, observer) = create_test_hooks_manager();
        let plan = create_mock_plan();
        let stage = create_mock_stage();
        let pipeline_node = create_mock_pipeline_node(1, 1, "test_plan_456");
        let plan_id = "nested_test";

        {
            let _plan_span = PlanSpan::new(&plan, plan_id, &hooks_manager);
            {
                let stage_span = StageSpan::new(&stage, plan_id, &hooks_manager);
                {
                    let _pipeline_span = stage_span.new_pipeline_span(pipeline_node);
                    let events = observer.get_events();
                    assert_eq!(events.len(), 3); // Plan, Stage, Pipeline started
                }
                let events = observer.get_events();
                assert_eq!(events.len(), 4); // + Pipeline completed
            }
            let events = observer.get_events();
            assert_eq!(events.len(), 5); // + Stage completed
        }

        let events = observer.get_events();
        assert_eq!(events.len(), 6); // + Plan completed
    }

    #[test]
    fn test_multiple_observers() {
        let observer1 = TestObserver::new();
        let observer2 = TestObserver::new();
        let hooks_manager = HooksManager {
            subscribers: vec![Arc::new(observer1.clone()), Arc::new(observer2.clone())],
        };

        let plan = create_mock_plan();
        let plan_id = "multi_observer_test";

        {
            let _span = PlanSpan::new(&plan, plan_id, &hooks_manager);
        }

        assert_eq!(observer1.get_events().len(), 2);
        assert_eq!(observer2.get_events().len(), 2);
    }

    // Mock creation helpers
    fn create_mock_plan() -> DistributedPhysicalPlan {
        let builder = dummy_scan_node(dummy_scan_operator(vec![Field::new(
            "test",
            DataType::Int8,
        )]));
        let config = Arc::new(DaftExecutionConfig::default());
        DistributedPhysicalPlan::from_logical_plan_builder(&builder, config)
            .expect("Failed to create mock plan")
    }

    fn create_mock_stage() -> Stage {
        create_mock_plan().stage_plan().get_root_stage().clone()
    }
    struct TestDistributedPipelineNode {
        stage_id: StageID,
        node_id: NodeID,
        plan_id: PlanID,
    }

    impl DistributedPipelineNode for TestDistributedPipelineNode {
        fn name(&self) -> &'static str {
            unimplemented!()
        }

        fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
            unimplemented!()
        }

        fn start(
            self: Arc<Self>,
            _: &mut crate::stage::StageContext,
        ) -> crate::pipeline_node::RunningPipelineNode {
            unimplemented!()
        }

        fn plan_id(&self) -> &crate::plan::PlanID {
            &self.plan_id
        }

        fn stage_id(&self) -> &crate::stage::StageID {
            &self.stage_id
        }

        fn node_id(&self) -> &crate::pipeline_node::NodeID {
            &self.node_id
        }

        fn as_tree_display(&self) -> &dyn common_display::tree::TreeDisplay {
            unimplemented!()
        }
    }

    fn create_mock_pipeline_node(
        stage_id: usize,
        node_id: usize,
        plan_id: &'static str,
    ) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(TestDistributedPipelineNode {
            stage_id: StageID::new(stage_id),
            node_id,
            plan_id: PlanID::from(plan_id),
        })
    }
}
