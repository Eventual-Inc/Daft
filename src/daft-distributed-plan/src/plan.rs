use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use common_scan_info::{PhysicalScanInfo, ScanState};
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanBuilder,
    LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

use crate::{
    dispatcher::TaskDispatcher,
    program::Program,
    stage::{CollectStage, Stage},
    worker::WorkerManager,
};

pub struct DistributedPhysicalPlan {
    remaining_logical_plan: Option<LogicalPlanRef>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlan {
    pub fn from_logical_plan_builder(
        builder: &LogicalPlanBuilder,
        config: &Arc<DaftExecutionConfig>,
    ) -> DaftResult<Self> {
        let plan = builder.build();
        if !can_translate_logical_plan(&plan) {
            return Err(DaftError::InternalError(
                "Cannot run this physical plan on distributed swordfish yet".to_string(),
            ));
        }

        Ok(Self {
            remaining_logical_plan: Some(plan),
            config: config.clone(),
        })
    }

    pub fn run_plan(
        &mut self,
        worker_manager: Arc<dyn WorkerManager>,
    ) -> DaftResult<impl Iterator<Item = DaftResult<Vec<PartitionRef>>>> {
        let remaining_plan = self.remaining_logical_plan.take().unwrap();
        let (stage, remaining_plan) =
            find_and_split_at_stage_boundary(&remaining_plan, &self.config)?;

        assert!(
            matches!(stage, Stage::Collect(_)),
            "We only support collect stages for now"
        );
        assert!(
            matches!(remaining_plan, None),
            "We expect no remaining plan for collect stage"
        );

        let dispatcher = TaskDispatcher::new(worker_manager);
        let program = Program::from_stage(stage, dispatcher, self.config.clone());
        Ok(program.run_program().into_iter())
    }
}

/// Find a stage boundary in the plan and split the plan at that point
/// This is still a WIP and will be extended to support more stage boundaries in the future
fn find_and_split_at_stage_boundary(
    plan: &LogicalPlanRef,
    config: &Arc<DaftExecutionConfig>,
) -> DaftResult<(Stage, Option<LogicalPlanRef>)> {
    struct StageBoundarySplitter {
        next_stage: Option<Stage>,
        _config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for StageBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            // TODO: Implement stage boundary splitting
            Ok(Transformed::no(node))
        }
    }

    let mut splitter = StageBoundarySplitter {
        next_stage: None,
        _config: config.clone(),
    };

    let transformed = plan.clone().rewrite(&mut splitter)?;

    if let Some(next_stage) = splitter.next_stage {
        Ok((next_stage, Some(transformed.data)))
    } else {
        // make collect stage
        let plan = transformed.data;
        let (plan, scan_infos, output_schemas) = replace_sources_with_placeholders(&plan)?;
        let scan_info = scan_infos.first().unwrap();
        let output_schema = output_schemas.first().unwrap();
        let collect_stage = CollectStage::new(
            scan_info.clone(),
            output_schema.clone(),
            plan,
            config.clone(),
        );
        Ok((Stage::Collect(collect_stage), None))
    }
}

fn can_translate_logical_plan(plan: &LogicalPlanRef) -> bool {
    match plan.as_ref() {
        LogicalPlan::Source(_) => true,
        LogicalPlan::Project(project) => can_translate_logical_plan(&project.input),
        LogicalPlan::ActorPoolProject(actor_pool_project) => {
            can_translate_logical_plan(&actor_pool_project.input)
        }
        LogicalPlan::Filter(filter) => can_translate_logical_plan(&filter.input),
        LogicalPlan::Sink(sink) => can_translate_logical_plan(&sink.input),
        LogicalPlan::Sample(sample) => can_translate_logical_plan(&sample.input),
        LogicalPlan::Explode(explode) => can_translate_logical_plan(&explode.input),
        LogicalPlan::Unpivot(unpivot) => can_translate_logical_plan(&unpivot.input),
        LogicalPlan::Pivot(_) => false,
        LogicalPlan::Limit(_) => false,
        LogicalPlan::Sort(_) => false,
        LogicalPlan::Distinct(_) => false,
        LogicalPlan::Aggregate(_) => false,
        LogicalPlan::Window(_) => false,
        LogicalPlan::Concat(_) => false,
        LogicalPlan::Intersect(_) => false,
        LogicalPlan::Union(_) => false,
        LogicalPlan::Join(_) => true,
        LogicalPlan::Repartition(_) => false,
        LogicalPlan::SubqueryAlias(_) => false,
        LogicalPlan::MonotonicallyIncreasingId(_) => false,
    }
}

pub(super) struct ReplaceSourcesWithPlaceholder {
    pub scan_infos: Vec<PhysicalScanInfo>,
    pub output_schemas: Vec<SchemaRef>,
}

impl TreeNodeRewriter for ReplaceSourcesWithPlaceholder {
    type Node = LogicalPlanRef;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            LogicalPlan::Source(source) => {
                let source_info = source.source_info.as_ref();
                if let SourceInfo::Physical(info) = source_info {
                    assert!(matches!(info.scan_state, ScanState::Tasks(_)));

                    self.scan_infos.push(info.clone());
                    self.output_schemas.push(source.output_schema.clone());

                    let ph = PlaceHolderInfo::new(
                        source.output_schema.clone(),
                        ClusteringSpec::default().into(),
                    );
                    let new_scan = LogicalPlan::Source(Source::new(
                        source.output_schema.clone(),
                        SourceInfo::PlaceHolder(ph).into(),
                    ));

                    Ok(Transformed::yes(new_scan.into()))
                } else {
                    Ok(Transformed::no(node))
                }
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

pub fn replace_sources_with_placeholders(
    plan: &LogicalPlanRef,
) -> DaftResult<(LogicalPlanRef, Vec<PhysicalScanInfo>, Vec<SchemaRef>)> {
    let mut replacer = ReplaceSourcesWithPlaceholder {
        scan_infos: Vec::new(),
        output_schemas: Vec::new(),
    };

    let transformed = plan.clone().rewrite(&mut replacer)?;
    Ok((
        transformed.data,
        replacer.scan_infos,
        replacer.output_schemas,
    ))
}
