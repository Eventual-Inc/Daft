use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::{Pushdowns, ScanState, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, InMemoryInfo, LogicalPlan, LogicalPlanRef,
    SourceInfo,
};

use crate::{
    pipeline_node::{
        in_memory_source::InMemorySourceNode, intermediate::IntermediateNode, limit::LimitNode,
        scan_source::ScanSourceNode, DistributedPipelineNode,
    },
    plan::PlanID,
    stage::StageID,
};

// Translate a logical plan to a distributed pipeline node.
pub(crate) fn logical_plan_to_pipeline_node(
    plan_id: PlanID,
    stage_id: StageID,
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
    let mut splitter = PipelineNodeBoundarySplitter::new(plan_id, stage_id, config, psets);
    let transformed = plan.rewrite(&mut splitter)?;
    splitter.finalize(transformed)
}

// PipelineNodeBoundarySplitter splits a logical plan based on pipeline node boundaries.
// It is used to create a pipeline node from a logical plan.
struct PipelineNodeBoundarySplitter {
    // The plan ID of the pipeline node.
    plan_id: PlanID,
    // The stage ID of the pipeline node.
    stage_id: StageID,
    // The current nodes in the pipeline.
    current_nodes: Vec<Arc<dyn DistributedPipelineNode>>,
    // The execution config.
    config: Arc<DaftExecutionConfig>,
    // The node ID counter.
    node_id_counter: usize,
    // The in memory partition sets.
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
}

impl PipelineNodeBoundarySplitter {
    fn new(
        plan_id: PlanID,
        stage_id: StageID,
        config: Arc<DaftExecutionConfig>,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            current_nodes: vec![],
            config,
            node_id_counter: 0,
            psets,
        }
    }

    fn get_next_node_id(&mut self) -> usize {
        let node_id = self.node_id_counter;
        self.node_id_counter += 1;
        node_id
    }

    fn add_intermediate_node(&mut self, local_plan: LocalPhysicalPlanRef) -> DaftResult<()> {
        self.add_node_from_fn(|self_, mut children, node_id| {
            assert!(children.len() == 1);
            let child = children.pop().unwrap();
            Ok(Arc::new(IntermediateNode::new(
                self_.plan_id.clone(),
                self_.stage_id.clone(),
                node_id,
                self_.config.clone(),
                local_plan,
                child,
            )))
        })
    }

    fn add_in_memory_source_node(
        &mut self,
        info: InMemoryInfo,
        local_plan: LocalPhysicalPlanRef,
    ) -> DaftResult<()> {
        self.add_node_from_fn(|self_, children, node_id| {
            assert!(children.is_empty());
            Ok(Arc::new(InMemorySourceNode::new(
                self_.plan_id.clone(),
                self_.stage_id.clone(),
                node_id,
                self_.config.clone(),
                info,
                local_plan,
                self_.psets.clone(),
            )))
        })
    }

    fn add_scan_source_node(
        &mut self,
        local_plan: LocalPhysicalPlanRef,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    ) -> DaftResult<()> {
        self.add_node_from_fn(|self_, children, node_id| {
            assert!(children.is_empty());
            Ok(Arc::new(ScanSourceNode::new(
                self_.plan_id.clone(),
                self_.stage_id.clone(),
                node_id,
                self_.config.clone(),
                local_plan,
                pushdowns,
                scan_tasks,
            )))
        })
    }

    fn add_node_from_fn(
        &mut self,
        create_node_fn: impl FnOnce(
            &Self,
            // The current nodes in the pipeline.
            Vec<Arc<dyn DistributedPipelineNode>>,
            // The node ID of the new node.
            usize,
        ) -> DaftResult<Arc<dyn DistributedPipelineNode>>,
    ) -> DaftResult<()> {
        let node_id = self.get_next_node_id();
        let children = std::mem::take(&mut self.current_nodes);
        let node = create_node_fn(self, children, node_id)?;
        self.current_nodes = vec![node];
        Ok(())
    }

    // Translate a logical plan to a pipeline node.
    // - If the current nodes is empty, it means that the node is the root of the pipeline, and either
    // an in memory source or a scan source will be created.
    // - If the current nodes is not empty, it means that an intermediate node will be created.
    fn add_node_from_plan(&mut self, logical_plan: LogicalPlanRef) -> DaftResult<()> {
        match self.current_nodes.len() {
            // If the current nodes is empty, it means that the node is the root of the pipeline, and
            // either an in memory source or a scan source will be created.
            0 => {
                let (logical_plan, inputs) = extract_inputs_from_logical_plan(logical_plan)?;
                let local_plan = translate(&logical_plan)?;
                match inputs {
                    PipelineInput::InMemorySource { info } => {
                        self.add_in_memory_source_node(info, local_plan)?
                    }
                    PipelineInput::ScanTasks {
                        pushdowns,
                        scan_tasks,
                    } => self.add_scan_source_node(local_plan, pushdowns, scan_tasks)?,
                }
                Ok(())
            }
            // If the current nodes is not empty, it means that an intermediate node will be created.
            1 => {
                let local_plan = translate(&logical_plan)?;
                self.add_intermediate_node(local_plan)?;
                Ok(())
            }
            _ => panic!("Nodes can currently only have one child"),
        }
    }

    // Finalize the pipeline node.
    // - If the transformed plan is a placeholder, it means that the logical plan has been fully translated,
    // and the current nodes should be returned.
    // - If the transformed plan is not a placeholder, it means that final pipeline node has not been created yet,
    // and we should create it.
    fn finalize(
        mut self,
        transformed: Transformed<LogicalPlanRef>,
    ) -> DaftResult<Arc<dyn DistributedPipelineNode>> {
        match transformed.data.as_ref() {
            LogicalPlan::Source(source)
                if matches!(source.source_info.as_ref(), SourceInfo::PlaceHolder(_)) =>
            {
                assert!(self.current_nodes.len() == 1);
                Ok(self.current_nodes.pop().expect("Expected exactly one node"))
            }
            _ => {
                self.add_node_from_plan(transformed.data)?;
                Ok(self.current_nodes.pop().expect("Expected exactly one node"))
            }
        }
    }
}

impl TreeNodeRewriter for PipelineNodeBoundarySplitter {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
        match node.as_ref() {
            LogicalPlan::Limit(limit) => {
                // 1. Create local limit node
                self.add_node_from_plan(node.clone())?;
                // 2. Create global limit node
                self.add_node_from_fn(|self_, mut children, node_id| {
                    assert!(children.len() == 1);
                    let input_node = children.pop().unwrap();
                    Ok(Arc::new(LimitNode::new(
                        self_.plan_id.clone(),
                        self_.stage_id.clone(),
                        node_id,
                        limit.limit as usize,
                        node.schema(),
                        self_.config.clone(),
                        input_node,
                    )))
                })?;
                // 3. Return placeholder
                let placeholder = PlaceHolderInfo::new(node.schema(), Default::default());
                let placeholder_source = LogicalPlan::Source(Source::new(
                    node.schema(),
                    Arc::new(SourceInfo::PlaceHolder(placeholder)),
                ));
                Ok(Transformed::yes(placeholder_source.into()))
            }
            #[cfg(feature = "python")]
            LogicalPlan::ActorPoolProject(project) => {
                // 1. Create input node if it is not a placeholder
                if !matches!(
                    project.input.as_ref(),
                    LogicalPlan::Source(source) if matches!(source.source_info.as_ref(), SourceInfo::PlaceHolder(_))
                ) {
                    self.add_node_from_plan(project.input.clone())?;
                }
                // 2. Create actor pool project node
                self.add_node_from_fn(|self_, mut children, node_id| {
                    assert!(children.len() == 1);
                    let input_node = children.pop().unwrap();
                    Ok(Arc::new(crate::pipeline_node::actor_udf::ActorUDF::new(
                        self_.plan_id.clone(),
                        self_.stage_id.clone(),
                        node_id,
                        self_.config.clone(),
                        project.projection.clone(),
                        project.input.schema(),
                        input_node,
                    )?))
                })?;
                // 3. Return placeholder
                let placeholder = PlaceHolderInfo::new(node.schema(), Default::default());
                let placeholder_source = LogicalPlan::Source(Source::new(
                    node.schema(),
                    Arc::new(SourceInfo::PlaceHolder(placeholder)),
                ));
                Ok(Transformed::yes(placeholder_source.into()))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

#[derive(Clone, Debug)]
enum PipelineInput {
    InMemorySource {
        info: InMemoryInfo,
    },
    ScanTasks {
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    },
}

fn extract_inputs_from_logical_plan(
    logical_plan: LogicalPlanRef,
) -> DaftResult<(LogicalPlanRef, PipelineInput)> {
    let mut pipeline_input = None;
    let transformed_plan = logical_plan.transform_up(|plan| match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::InMemory(info) => {
                pipeline_input = Some(PipelineInput::InMemorySource { info: info.clone() });
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop))
            }
            SourceInfo::Physical(info) => {
                let pushdowns = info.pushdowns.clone();
                let scan_tasks = match &info.scan_state {
                    ScanState::Tasks(tasks) => tasks.clone(),
                    ScanState::Operator(_) => unreachable!("ScanState::Operator should not be present in the logical plan after optimization"),
                };
                pipeline_input = Some(PipelineInput::ScanTasks {
                    pushdowns,
                    scan_tasks,
                });
                let placeholder =
                    PlaceHolderInfo::new(source.output_schema.clone(), Default::default());
                let placeholder_source = LogicalPlan::Source(Source::new(
                    source.output_schema.clone(),
                    Arc::new(SourceInfo::PlaceHolder(placeholder)),
                ));
                Ok(Transformed::new(
                    placeholder_source.into(),
                    true,
                    TreeNodeRecursion::Stop,
                ))
            }
            SourceInfo::PlaceHolder(_) => Ok(Transformed::new(plan, true, TreeNodeRecursion::Stop)),
        },
        _ => Ok(Transformed::no(plan)),
    })?;

    Ok((
        transformed_plan.data,
        pipeline_input.expect("Expected pipeline input from logical plan"),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_scan_info::{test::DummyScanOperator, ScanOperatorRef};
    use daft_dsl::{lit, resolved_col};
    use daft_logical_plan::{
        logical_plan::LogicalPlan, ops::Source, source_info::SourceInfo, LogicalPlanBuilder,
    };
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;

    fn dummy_in_memory_scan(fields: Vec<Field>) -> DaftResult<LogicalPlanBuilder> {
        let schema = Arc::new(Schema::new(fields));

        let source_info = SourceInfo::InMemory(InMemoryInfo::new(
            schema.clone(),
            "".into(),
            None,
            1,
            0,
            0,
            None,
            None,
        ));
        let logical_plan: LogicalPlan = Source::new(schema, source_info.into()).into();

        Ok(LogicalPlanBuilder::from(Arc::new(logical_plan)))
    }

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

    #[test]
    fn test_logical_in_memory_source_to_pipeline() {
        let fields = vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ];
        let plan = dummy_in_memory_scan(fields).unwrap().build();
        let plan_id = Arc::from("foo");
        let stage_id = StageID::new(0);
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_id,
            stage_id,
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "DistributedInMemoryScan");
        assert_eq!(pipeline_node.children().len(), 0);
    }

    #[test]
    fn test_logical_scan_source_to_pipeline() {
        let fields = vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(fields))
            .optimize()
            .unwrap() // To fill scan node with tasks
            .build();
        eprintln!("{}", plan.repr_ascii(false));
        let plan_id = Arc::from("foo");
        let stage_id = StageID::new(0);
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_id,
            stage_id,
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "DistributedScan");
        assert_eq!(pipeline_node.children().len(), 0);
    }

    #[test]
    fn test_logical_limit_to_pipeline() -> DaftResult<()> {
        let fields = vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(fields))
            .limit(20, false)?
            .optimize()? // To fill scan node with tasks
            .build();
        let plan_id = Arc::from("foo");
        let stage_id = StageID::new(0);
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_id,
            stage_id,
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "DistributedLimit");

        let children = pipeline_node.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "DistributedScan");
        assert_eq!(children[0].children().len(), 0);

        Ok(())
    }

    #[test]
    fn test_logical_project_to_pipeline() -> DaftResult<()> {
        let fields = vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(fields))
            .with_columns(vec![resolved_col("group")
                .add(resolved_col("value"))
                .alias("group_value")])?
            .optimize()? // To fill scan node with tasks
            .build();
        let plan_id = Arc::from("foo");
        let stage_id = StageID::new(0);
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_id,
            stage_id,
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "DistributedScan");
        assert_eq!(pipeline_node.children().len(), 0);

        Ok(())
    }

    #[test]
    fn test_map_logical_plan_to_pipeline() -> DaftResult<()> {
        let fields = vec![
            Field::new("category", DataType::Utf8),
            Field::new("group", DataType::Int64),
            Field::new("value", DataType::Int64),
        ];
        let plan = dummy_scan_node(dummy_scan_operator(fields))
            .optimize()? // To fill scan node with tasks
            .with_columns(vec![resolved_col("group")
                .add(resolved_col("value"))
                .alias("group_value")])?
            .filter(resolved_col("group_value").eq(lit(0)))?
            .limit(20, false)?
            .select(vec![resolved_col("group_value")])?
            .build();
        let plan_id = Arc::from("foo");
        let stage_id = StageID::new(0);
        let pipeline_node = logical_plan_to_pipeline_node(
            plan_id,
            stage_id,
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        // Intermediate <- Limit <- Source
        assert_eq!(pipeline_node.name(), "DistributedIntermediateNode");

        let children = pipeline_node.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "DistributedLimit");

        let children = children[0].children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "DistributedScan");
        assert_eq!(children[0].children().len(), 0);

        Ok(())
    }
}
