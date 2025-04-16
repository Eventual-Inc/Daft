use std::{cmp::min, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_scan_info::{PartitionField, PhysicalScanInfo, Pushdowns, ScanState, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_local_plan::{translate, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source, optimization::OptimizerBuilder, source_info::PlaceHolderInfo, ClusteringSpec,
    LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;

struct SourcePlanProducer {
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    source_schema: SchemaRef,
    output_schema: SchemaRef,
    partitioning_keys: Vec<PartitionField>,
    pushdowns: Pushdowns,
    next_source: usize,
    config: Arc<DaftExecutionConfig>,
}

impl SourcePlanProducer {
    const DEFAULT_MEMORY_COST: usize = 1024 * 1024 * 1024 * 16; // 4GB

    pub fn new(
        source: PhysicalScanInfo,
        output_schema: SchemaRef,
        config: Arc<DaftExecutionConfig>,
    ) -> Self {
        let scan_tasks = match source.scan_state {
            ScanState::Operator(_) => panic!("Operator scan state not supported"),
            ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
        };
        Self {
            scan_tasks,
            source_schema: source.source_schema,
            output_schema,
            partitioning_keys: source.partitioning_keys,
            pushdowns: source.pushdowns,
            next_source: 0,
            config,
        }
    }

    pub fn next_plan(&mut self) -> Option<LogicalPlanRef> {
        let mut start = self.next_source;
        if start >= self.scan_tasks.len() {
            return None;
        }

        let mut scan_tasks = Vec::new();
        let mut scan_task_total_memory_cost = 0;

        while scan_task_total_memory_cost < Self::DEFAULT_MEMORY_COST
            && start < self.scan_tasks.len()
        {
            let scan_task = self.scan_tasks[start].clone();
            scan_task_total_memory_cost += scan_task
                .estimate_in_memory_size_bytes(Some(&self.config))
                .unwrap_or(Self::DEFAULT_MEMORY_COST);
            scan_tasks.push(scan_task);
            start += 1;
        }

        self.next_source = start;

        println!("scan_tasks: {:?}", scan_tasks.len());
        let source = PhysicalScanInfo {
            scan_state: ScanState::Tasks(scan_tasks.into()),
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns: self.pushdowns.clone(),
        };
        Some(
            LogicalPlan::Source(Source::new(
                self.output_schema.clone(),
                SourceInfo::Physical(source).into(),
            ))
            .into(),
        )
    }

    pub fn has_remaining_plans(&self) -> bool {
        self.next_source < self.scan_tasks.len()
    }
}

pub struct DistributedPhysicalPlanner {
    logical_plan: LogicalPlanRef,
    source_producer: SourcePlanProducer,
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
        let mut replacer = ReplaceSourcesWithPlaceholder {
            scan_infos: Vec::new(),
            output_schemas: Vec::new(),
        };
        let replaced = plan.rewrite(&mut replacer)?;
        let plan = replaced.data;
        let inputs = replacer.scan_infos;
        assert_eq!(inputs.len(), 1); // for now we only support map pipelines, so only 1 source

        Ok(Self {
            logical_plan: plan,
            source_producer: SourcePlanProducer::new(
                inputs[0].clone(),
                replacer.output_schemas[0].clone(),
                config.clone(),
            ),
        })
    }

    pub fn next_plan(&mut self) -> DaftResult<Option<LocalPhysicalPlanRef>> {
        if let Some(source) = self.source_producer.next_plan() {
            let next_logical_plan =
                replace_placeholders_with_sources(self.logical_plan.clone(), source)?;
            let optimizer = OptimizerBuilder::new().enrich_with_stats().build();
            let optimized_logical_plan =
                optimizer.optimize(next_logical_plan, |_, _, _, _, _| {})?;
            let local_physical_plan = translate(&optimized_logical_plan)?;
            Ok(Some(local_physical_plan))
        } else {
            Ok(None)
        }
    }

    pub fn has_remaining_plans(&self) -> bool {
        self.source_producer.has_remaining_plans()
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

fn replace_placeholders_with_sources(
    plan: LogicalPlanRef,
    new_source_plan: LogicalPlanRef,
) -> DaftResult<LogicalPlanRef> {
    let new_plan = plan.transform_up(|plan| match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::PlaceHolder(ph) => Ok(Transformed::yes(new_source_plan.clone())),
            _ => Ok(Transformed::no(plan)),
        },
        _ => Ok(Transformed::no(plan)),
    })?;
    Ok(new_plan.data)
}
