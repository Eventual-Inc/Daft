use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_scan_info::{PhysicalScanInfo, ScanState};
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanBuilder,
    LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

use crate::stage::{CollectStage, LimitStage, SwordfishStage};

pub struct DistributedPhysicalPlanner {
    remaining_logical_plan: Option<LogicalPlanRef>,
    config: Arc<DaftExecutionConfig>,
}

impl DistributedPhysicalPlanner {
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

    pub fn next_stage(&mut self) -> DaftResult<Option<Box<dyn SwordfishStage + Send + Sync>>> {
        println!("getting next stage");
        if self.remaining_logical_plan.is_none() {
            return Ok(None);
        }

        let remaining_plan = match self.remaining_logical_plan.take() {
            Some(plan) => plan,
            None => return Ok(None),
        };

        // Analyze the plan to see if we have a stage boundary (currently just limit)
        let (next_stage, remaining_plan) =
            find_and_split_at_stage_boundary(&remaining_plan, &self.config)?;

        self.remaining_logical_plan = remaining_plan;

        println!("next stage: {:#?}", next_stage);
        println!("remaining plan: {:#?}", self.remaining_logical_plan);

        Ok(Some(next_stage))
    }

    pub fn is_done(&self) -> bool {
        self.remaining_logical_plan.is_none()
    }
}

/// Find a stage boundary in the plan and split the plan at that point
/// Currently only finds limit operations, but can be extended for other stage boundaries
/// Returns a StageBoundary with boundary-specific information if found
fn find_and_split_at_stage_boundary(
    plan: &LogicalPlanRef,
    config: &Arc<DaftExecutionConfig>,
) -> DaftResult<(
    Box<dyn SwordfishStage + Send + Sync>,
    Option<LogicalPlanRef>,
)> {
    struct StageBoundarySplitter {
        next_stage: Option<Box<dyn SwordfishStage + Send + Sync>>,
        config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for StageBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    println!("found limit stage");
                    let schema = node.schema();
                    let ph = PlaceHolderInfo::new(schema.clone(), ClusteringSpec::default().into());
                    let placeholder = LogicalPlan::Source(Source::new(
                        schema,
                        SourceInfo::PlaceHolder(ph).into(),
                    ));

                    let (plan, scan_infos, output_schemas) =
                        replace_sources_with_placeholders(&node)?;
                    let scan_info = scan_infos.first().unwrap();
                    let output_schema = output_schemas.first().unwrap();

                    self.next_stage = Some(Box::new(LimitStage::new(
                        scan_info.clone(),
                        output_schema.clone(),
                        plan,
                        limit.limit as usize,
                        self.config.clone(),
                    )));

                    Ok(Transformed::new(
                        placeholder.into(),
                        true,
                        common_treenode::TreeNodeRecursion::Stop,
                    ))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = StageBoundarySplitter {
        next_stage: None,
        config: config.clone(),
    };

    let transformed = plan.clone().rewrite(&mut splitter)?;

    if let Some(next_stage) = splitter.next_stage {
        Ok((next_stage, Some(transformed.data)))
    } else {
        // make collect stage
        let plan = transformed.data;
        println!("making collect stage for plan: {:#?}", plan);
        let (plan, scan_infos, output_schemas) = replace_sources_with_placeholders(&plan)?;
        let scan_info = scan_infos.first().unwrap();
        println!("scan info: {:#?}", scan_info);
        let output_schema = output_schemas.first().unwrap();
        let collect_stage = Box::new(CollectStage::new(
            scan_info.clone(),
            output_schema.clone(),
            plan,
            config.clone(),
        ));
        Ok((collect_stage, None))
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
        LogicalPlan::Pivot(pivot) => can_translate_logical_plan(&pivot.input),
        LogicalPlan::Limit(limit) => can_translate_logical_plan(&limit.input),
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

pub fn replace_placeholders_with_sources(
    plan: LogicalPlanRef,
    new_source_plan: LogicalPlanRef,
) -> DaftResult<LogicalPlanRef> {
    let new_plan = plan.transform_up(|plan| match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::PlaceHolder(_ph) => Ok(Transformed::yes(new_source_plan.clone())),
            _ => Ok(Transformed::no(plan)),
        },
        _ => Ok(Transformed::no(plan)),
    })?;
    Ok(new_plan.data)
}
