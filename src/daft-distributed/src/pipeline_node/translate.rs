use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::{Pushdowns, ScanState, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use daft_local_plan::translate;
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, InMemoryInfo, LogicalPlan,
    LogicalPlanRef, SourceInfo,
};

use crate::pipeline_node::{
    in_memory_source::InMemorySourceNode, intermediate::IntermediateNode, limit::LimitNode,
    scan_source::ScanSourceNode, DistributedPipelineNode,
};

pub(crate) fn logical_plan_to_pipeline_node(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: Arc<HashMap<String, Vec<PartitionRef>>>,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    #[allow(dead_code)]
    struct PipelineNodeBoundarySplitter {
        root: LogicalPlanRef,
        current_nodes: Vec<Box<dyn DistributedPipelineNode>>,
        config: Arc<DaftExecutionConfig>,
        node_id_counter: usize,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    }

    impl PipelineNodeBoundarySplitter {
        fn get_next_node_id(&mut self) -> usize {
            let node_id = self.node_id_counter;
            self.node_id_counter += 1;
            node_id
        }

        fn create_node(
            &mut self,
            logical_plan: LogicalPlanRef,
            current_nodes: Vec<Box<dyn DistributedPipelineNode>>,
        ) -> DaftResult<Box<dyn DistributedPipelineNode>> {
            // If current_nodes is not empty, create an intermediate node immediately
            if !current_nodes.is_empty() {
                let translated = translate(&logical_plan)?;
                return Ok(Box::new(IntermediateNode::new(
                    self.get_next_node_id(),
                    self.config.clone(),
                    translated,
                    current_nodes,
                )) as Box<dyn DistributedPipelineNode>);
            }

            // Otherwise create a node based on pipeline_input
            let (logical_plan, inputs) = extract_inputs_from_logical_plan(logical_plan)?;
            let translated = translate(&logical_plan)?;
            let node = match inputs {
                PipelineInput::InMemorySource { info } => Box::new(InMemorySourceNode::new(
                    self.get_next_node_id(),
                    self.config.clone(),
                    info,
                    translated,
                    self.psets.clone(),
                ))
                    as Box<dyn DistributedPipelineNode>,
                PipelineInput::ScanTasks {
                    pushdowns,
                    scan_tasks,
                } => Box::new(ScanSourceNode::new(
                    self.get_next_node_id(),
                    self.config.clone(),
                    translated,
                    pushdowns,
                    scan_tasks,
                )) as Box<dyn DistributedPipelineNode>,
            };
            Ok(node)
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
                    let current_nodes = std::mem::take(&mut self.current_nodes);
                    let input_node = self.create_node(node.clone(), current_nodes)?;
                    let limit_node = Box::new(LimitNode::new(
                        self.get_next_node_id(),
                        limit.limit as usize,
                        node.schema(),
                        self.config.clone(),
                        input_node,
                    ));

                    self.current_nodes = vec![limit_node];
                    // Here we will have to return a placeholder, essentially cutting off the plan
                    let placeholder =
                        PlaceHolderInfo::new(node.schema(), ClusteringSpec::default().into());
                    Ok(Transformed::yes(
                        LogicalPlan::Source(Source::new(
                            node.schema(),
                            SourceInfo::PlaceHolder(placeholder).into(),
                        ))
                        .into(),
                    ))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = PipelineNodeBoundarySplitter {
        root: plan.clone(),
        current_nodes: vec![],
        config,
        node_id_counter: 0,
        psets,
    };

    let transformed = plan.rewrite(&mut splitter)?;
    match transformed.data.as_ref() {
        LogicalPlan::Source(source)
            if matches!(source.source_info.as_ref(), SourceInfo::PlaceHolder(_)) =>
        {
            assert!(splitter.current_nodes.len() == 1);
            Ok(splitter
                .current_nodes
                .pop()
                .expect("Expected exactly one node"))
        }
        _ => {
            let logical_plan = transformed.data;
            let current_nodes = std::mem::take(&mut splitter.current_nodes);
            let node = splitter.create_node(logical_plan, current_nodes)?;
            Ok(node)
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
                    ScanState::Operator(_) => unreachable!(),
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
        pipeline_input.expect("Expected pipeline input"),
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

        let pipeline_node = logical_plan_to_pipeline_node(
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "InMemorySourceNode");
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

        let pipeline_node = logical_plan_to_pipeline_node(
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "ScanSource");
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

        let pipeline_node = logical_plan_to_pipeline_node(
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "Limit");

        let children = pipeline_node.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "ScanSource");
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

        let pipeline_node = logical_plan_to_pipeline_node(
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(pipeline_node.name(), "ScanSource");
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

        let pipeline_node = logical_plan_to_pipeline_node(
            plan,
            Arc::new(DaftExecutionConfig::default()),
            Arc::new(HashMap::new()),
        )
        .unwrap();

        // Intermediate <- Limit <- Source
        assert_eq!(pipeline_node.name(), "Intermediate");

        let children = pipeline_node.children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "Limit");

        let children = children[0].children();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name(), "ScanSource");
        assert_eq!(children[0].children().len(), 0);

        Ok(())
    }
}
