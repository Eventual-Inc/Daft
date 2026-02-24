use std::{borrow::Cow, collections::HashMap, fmt::Display, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{
    DisplayLevel,
    ascii::fmt_tree_gitstyle,
    mermaid::{MermaidDisplayVisitor, SubgraphOptions},
    tree::TreeDisplay,
};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use daft_core::{
    join::JoinSide,
    prelude::{Schema, SchemaRef},
};
use daft_dsl::{common_treenode::ConcreteTreeNode, join::get_common_join_cols};
use daft_local_plan::{
    CommitWrite, Concat, CrossJoin, Dedup, EmptyScan, Explode, Filter, GlobScan, HashAggregate,
    HashJoin, InMemoryScan, IntoBatches, Limit, LocalNodeContext, LocalPhysicalPlan,
    MonotonicallyIncreasingId, PhysicalWrite, Pivot, Project, Sample, Sort, SortMergeJoin, TopN,
    UDFProject, UnGroupedAggregate, Unpivot, VLLMProject, WindowOrderByOnly,
    WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy, WindowPartitionOnly,
};
use daft_logical_plan::{JoinType, stats::StatsState};
use daft_micropartition::{
    MicroPartition, MicroPartitionRef,
    partitioning::{MicroPartitionSet, PartitionSetCache},
};
use daft_scan::ScanTaskRef;
use daft_writers::make_physical_writer_factory;
use indexmap::IndexSet;
use snafu::ResultExt;

use crate::{
    ExecutionRuntimeContext, PipelineCreationSnafu,
    channel::Receiver,
    intermediate_ops::{
        cross_join::CrossJoinOperator,
        distributed_actor_pool_project::DistributedActorPoolProjectOperator,
        explode::ExplodeOperator, filter::FilterOperator,
        inner_hash_join_probe::InnerHashJoinProbeOperator, intermediate_op::IntermediateNode,
        into_batches::IntoBatchesOperator, project::ProjectOperator, udf::UdfOperator,
        unpivot::UnpivotOperator,
    },
    runtime_stats::RuntimeStats,
    sinks::{
        aggregate::AggregateSink,
        blocking_sink::BlockingSinkNode,
        commit_write::CommitWriteSink,
        dedup::DedupSink,
        grouped_aggregate::GroupedAggregateSink,
        hash_join_build::HashJoinBuildSink,
        into_partitions::IntoPartitionsSink,
        join_collect::JoinCollectSink,
        pivot::PivotSink,
        repartition::RepartitionSink,
        sort::SortSink,
        top_n::TopNSink,
        window_order_by_only::WindowOrderByOnlySink,
        window_partition_and_dynamic_frame::WindowPartitionAndDynamicFrameSink,
        window_partition_and_order_by::WindowPartitionAndOrderBySink,
        window_partition_only::WindowPartitionOnlySink,
        write::{WriteFormat, WriteSink},
    },
    sources::{empty_scan::EmptyScanSource, in_memory::InMemorySource, source::SourceNode},
    state_bridge::BroadcastStateBridge,
    streaming_sink::{
        anti_semi_hash_join_probe::AntiSemiProbeSink, async_udf::AsyncUdfSink,
        base::StreamingSinkNode, concat::ConcatSink, limit::LimitSink,
        monotonically_increasing_id::MonotonicallyIncreasingIdSink,
        outer_hash_join_probe::OuterHashJoinProbeSink, sample::SampleSink,
        sort_merge_join_probe::SortMergeJoinProbe, vllm::VLLMSink,
    },
};

pub type NodeName = Cow<'static, str>;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MorselSizeRequirement {
    // Fixed size morsel
    Strict(usize),
    // Flexible size morsel, between lower and upper bound
    Flexible(usize, usize),
}

impl Display for MorselSizeRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict(size) => write!(f, "{}", size),
            Self::Flexible(lower, upper) => write!(f, "Range({lower}, {upper}]"),
        }
    }
}

impl Default for MorselSizeRequirement {
    fn default() -> Self {
        Self::Flexible(
            0,
            daft_context::get_context()
                .execution_config()
                .default_morsel_size,
        )
    }
}

impl MorselSizeRequirement {
    pub fn combine_requirements(
        current_requirement: Option<Self>,
        downstream_requirement: Self,
    ) -> Self {
        match (current_requirement, downstream_requirement) {
            // If there is no current requirement and the downstream requirement is strict, use flexible with 0 as lower bound
            (None, Self::Strict(size)) => Self::Flexible(0, size),
            // If there is no current requirement and the downstream requirement is flexible, use flexible with 0 as lower bound
            (None, Self::Flexible(_, upper)) => Self::Flexible(0, upper),
            // If the current requirement is strict use it regardless of the downstream requirement
            (Some(Self::Strict(current_size)), _) => Self::Strict(current_size),
            // If the current requirement is flexible and the downstream requirement is strict, use the minimum of the two sizes
            (
                Some(Self::Flexible(lower_flexible_size, upper_flexible_size)),
                Self::Strict(strict_size),
            ) => Self::Flexible(
                lower_flexible_size.min(strict_size),
                strict_size.min(upper_flexible_size),
            ),
            // If the current requirement is flexible and the downstream requirement is flexible, use the intersection of ranges
            (
                Some(Self::Flexible(lower_flexible_size, upper_flexible_size)),
                Self::Flexible(lower_other_size, upper_other_size),
            ) => {
                let lower = lower_flexible_size.max(lower_other_size);
                let upper = upper_flexible_size.min(upper_other_size);

                // If ranges don't overlap, fall back to downstream requirement
                if lower > upper {
                    Self::Flexible(lower_other_size, upper_other_size)
                } else {
                    Self::Flexible(lower, upper)
                }
            }
        }
    }

    pub fn values(&self) -> (usize, usize) {
        match self {
            Self::Strict(size) => (*size, *size),
            Self::Flexible(lower, upper) => (*lower, *upper),
        }
    }
}

pub(crate) trait PipelineNode: Sync + Send + TreeDisplay {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    #[allow(clippy::borrowed_box)]
    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>>;
    fn name(&self) -> Arc<str>;
    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    );
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>>;

    fn as_tree_display(&self) -> &dyn TreeDisplay;

    /// Unique id to identify which plan all nodes belong to
    fn plan_id(&self) -> Arc<str>;
    /// Unique id to identify the node.
    fn node_id(&self) -> usize;
    // General Node Info
    fn node_info(&self) -> Arc<NodeInfo>;
    // Runtime Stats
    fn runtime_stats(&self) -> Arc<dyn RuntimeStats>;
}

impl ConcreteTreeNode for Box<dyn PipelineNode> {
    fn children(&self) -> Vec<&Self> {
        self.boxed_children()
    }

    fn take_children(self) -> (Self, Vec<Self>) {
        unimplemented!("with_new_children is not supported for Box<dyn PipelineNode>")
    }

    fn with_new_children(self, _children: Vec<Self>) -> DaftResult<Self> {
        unimplemented!("with_new_children is not supported for Box<dyn PipelineNode>")
    }
}

/// Single use context for translating a physical plan to a Pipeline.
/// It generates a plan_id, and node ids for each plan.
pub struct RuntimeContext {
    index_counter: std::cell::RefCell<usize>,
    context: HashMap<String, String>,
}

impl RuntimeContext {
    pub fn new() -> Self {
        Self::new_with_context(HashMap::new())
    }

    pub fn new_with_context(context: HashMap<String, String>) -> Self {
        Self {
            index_counter: std::cell::RefCell::new(0),
            context,
        }
    }

    pub fn next_id(&self) -> usize {
        let mut counter = self.index_counter.borrow_mut();
        let index = *counter;
        *counter += 1;
        index
    }

    pub fn next_node_info(
        &self,
        name: Arc<str>,
        node_type: NodeType,
        node_category: NodeCategory,
        output_schema: SchemaRef,
        node_context: &LocalNodeContext,
    ) -> NodeInfo {
        let context = if let Some(node_context) = &node_context.additional {
            node_context
                .iter()
                .chain(self.context.iter())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            self.context.clone()
        };

        NodeInfo {
            name,
            id: node_context
                .origin_node_id
                .unwrap_or_else(|| self.next_id()),
            node_type,
            node_category,
            context,
            output_schema,
        }
    }
}

pub fn viz_pipeline_mermaid(
    root: &dyn PipelineNode,
    display_type: DisplayLevel,
    bottom_up: bool,
    subgraph_options: Option<SubgraphOptions>,
) -> String {
    let mut output = String::new();
    let mut visitor =
        MermaidDisplayVisitor::new(&mut output, display_type, bottom_up, subgraph_options);
    visitor.fmt(root.as_tree_display()).unwrap();
    output
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyo3::pyclass)]
#[allow(dead_code)]
pub struct RelationshipNode {
    pub id: usize,
    pub parent_id: Option<usize>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyo3::pyclass)]
#[allow(dead_code)]
pub struct RelationshipInformation {
    pub ids: Vec<RelationshipNode>,
    pub plan_id: Arc<str>,
}
/// Performs a depth first pre-order traversal of the pipeline tree.
/// Returning a list of ids for each node traversed
/// For example, given the following pipeline with ids of:
/// ```
///                  1
///              /   |   \
///             2    3    4
///           /  \   |   / \
///          5    6  7  8   10
///                    /
///                   9
/// ```
/// The result would be [1, 2, 5, 6, 3, 7, 4, 8, 9, 10]
/// as we visit each node in pre-order traversal.
pub fn get_pipeline_relationship_mapping(root: &dyn PipelineNode) -> RelationshipInformation {
    let mut nodes = Vec::new();

    fn traverse(
        node: &dyn PipelineNode,
        parent_id: Option<usize>,
        nodes: &mut Vec<RelationshipNode>,
    ) {
        let current_id = node.node_id();
        nodes.push(RelationshipNode {
            id: current_id,
            parent_id,
        });

        for child in node.children() {
            traverse(child, Some(current_id), nodes);
        }
    }

    traverse(root, None, &mut nodes);
    RelationshipInformation {
        ids: nodes,
        plan_id: root.plan_id(),
    }
}

pub fn viz_pipeline_ascii(root: &dyn PipelineNode, simple: bool) -> String {
    let mut s = String::new();
    let level = if simple {
        DisplayLevel::Compact
    } else {
        DisplayLevel::Default
    };
    fmt_tree_gitstyle(root.as_tree_display(), 0, &mut s, level).unwrap();
    s
}

pub fn translate_physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
    cfg: &Arc<DaftExecutionConfig>,
    ctx: &RuntimeContext,
) -> crate::Result<Box<dyn PipelineNode>> {
    let mut pipeline_node = physical_plan_to_pipeline(physical_plan, psets, cfg, ctx)?;
    pipeline_node.propagate_morsel_size_requirement(
        MorselSizeRequirement::Flexible(0, cfg.default_morsel_size),
        MorselSizeRequirement::Flexible(0, cfg.default_morsel_size),
    );
    Ok(pipeline_node)
}

fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
    cfg: &Arc<DaftExecutionConfig>,
    ctx: &RuntimeContext,
) -> crate::Result<Box<dyn PipelineNode>> {
    use daft_local_plan::PhysicalScan;

    use crate::sources::scan_task::ScanTaskSource;
    let pipeline_node: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::PlaceholderScan(_) => {
            panic!("PlaceholderScan should not be converted to a pipeline node")
        }
        LocalPhysicalPlan::EmptyScan(EmptyScan {
            schema,
            stats_state,
            context,
        }) => {
            let source = EmptyScanSource::new(schema.clone());
            SourceNode::new(
                source.arced(),
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            stats_state,
            context,
        }) => {
            let scan_tasks = scan_tasks
                .iter()
                .map(|task| task.clone().as_any_arc().downcast().unwrap())
                .collect::<Vec<ScanTaskRef>>();

            let scan_task_source =
                ScanTaskSource::new(scan_tasks, pushdowns.clone(), schema.clone(), cfg);
            SourceNode::new(
                scan_task_source.arced(),
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowPartitionOnly(WindowPartitionOnly {
            input,
            partition_by,
            schema,
            stats_state,
            aggregations,
            aliases,
            context,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let window_partition_only_sink =
                WindowPartitionOnlySink::new(aggregations, aliases, partition_by, schema)
                    .with_context(|_| PipelineCreationSnafu {
                        plan_name: physical_plan.name(),
                    })?;
            BlockingSinkNode::new(
                Arc::new(window_partition_only_sink),
                input_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            functions,
            aliases,
            context,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let window_partition_and_order_by_sink = WindowPartitionAndOrderBySink::new(
                functions,
                aliases,
                partition_by,
                order_by,
                descending,
                nulls_first,
                schema,
            )
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(window_partition_and_order_by_sink),
                input_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
            input,
            partition_by,
            order_by,
            descending,
            nulls_first,
            frame,
            min_periods,
            schema,
            stats_state,
            aggregations,
            aliases,
            context,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let window_partition_and_dynamic_frame_sink = WindowPartitionAndDynamicFrameSink::new(
                aggregations,
                *min_periods,
                aliases,
                partition_by,
                order_by,
                descending,
                nulls_first,
                frame,
                schema,
            )
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(window_partition_and_dynamic_frame_sink),
                input_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowOrderByOnly(WindowOrderByOnly {
            input,
            order_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            functions,
            aliases,
            context,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let window_order_by_only_op = WindowOrderByOnlySink::new(
                functions,
                aliases,
                order_by,
                descending,
                nulls_first,
                schema,
            )
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(window_order_by_only_op),
                input_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan {
            info,
            stats_state,
            context,
        }) => {
            let cache_key: Arc<str> = info.cache_key.clone().into();

            let materialized_pset = psets.get_partition_set(&cache_key);
            let in_memory_source = InMemorySource::new(
                materialized_pset,
                info.source_schema.clone(),
                info.size_bytes,
            )
            .arced();
            SourceNode::new(
                in_memory_source,
                stats_state.clone(),
                ctx,
                info.source_schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Project(Project {
            input,
            projection,
            schema,
            stats_state,
            context,
        }) => {
            let proj_op = ProjectOperator::new(projection.clone()).with_context(|_| {
                PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                }
            })?;
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(proj_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::UDFProject(UDFProject {
            input,
            expr,
            udf_properties,
            passthrough_columns,
            stats_state,
            schema,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            if udf_properties.is_async
                && udf_properties.concurrency.is_none()
                && !udf_properties.use_process.unwrap_or(false)
            {
                let async_sink = AsyncUdfSink::new(
                    expr.clone(),
                    udf_properties.clone(),
                    passthrough_columns.clone(),
                    schema,
                );
                StreamingSinkNode::new(
                    Arc::new(async_sink),
                    vec![child_node],
                    stats_state.clone(),
                    ctx,
                    schema.clone(),
                    context,
                )
                .boxed()
            } else {
                let proj_op = UdfOperator::try_new(
                    expr.clone(),
                    udf_properties.clone(),
                    passthrough_columns.clone(),
                    schema,
                    input.schema(),
                )
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;
                IntermediateNode::new(
                    Arc::new(proj_op),
                    vec![child_node],
                    stats_state.clone(),
                    ctx,
                    schema.clone(),
                    context,
                )
                .boxed()
            }
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::DistributedActorPoolProject(
            daft_local_plan::DistributedActorPoolProject {
                input,
                actor_objects,
                batch_size,
                memory_request,
                schema,
                stats_state,
                context,
            },
        ) => {
            let distributed_actor_pool_project_op = DistributedActorPoolProjectOperator::try_new(
                actor_objects.clone(),
                *batch_size,
                *memory_request,
            )
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(distributed_actor_pool_project_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Sample(Sample {
            input,
            sampling_method,
            with_replacement,
            seed,
            schema,
            stats_state,
            context,
        }) => {
            let sample_sink =
                SampleSink::new(*sampling_method, *with_replacement, *seed, schema.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            StreamingSinkNode::new(
                Arc::new(sample_sink),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input,
            predicate,
            schema,
            stats_state,
            context,
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(filter_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::IntoBatches(IntoBatches {
            input,
            batch_size,
            strict,
            schema,
            stats_state,
            context,
        }) => {
            let into_batches_op = IntoBatchesOperator::new(*batch_size, *strict);
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(into_batches_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Explode(Explode {
            input,
            to_explode,
            schema,
            stats_state,
            context,
        }) => {
            let explode_op = ExplodeOperator::new(to_explode.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(explode_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input,
            limit,
            offset,
            schema,
            stats_state,
            context,
        }) => {
            let (offset, limit) = (*offset, *limit);
            let sink = LimitSink::new(limit as usize, offset.map(|x| x as usize));
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            StreamingSinkNode::new(
                Arc::new(sink),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Concat(Concat {
            input,
            other,
            schema,
            stats_state,
            context,
        }) => {
            let left_child = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let right_child = physical_plan_to_pipeline(other, psets, cfg, ctx)?;
            let sink = ConcatSink {};
            StreamingSinkNode::new(
                Arc::new(sink),
                vec![left_child, right_child],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let agg_sink = AggregateSink::new(aggregations, input.schema()).with_context(|_| {
                PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                }
            })?;
            BlockingSinkNode::new(
                Arc::new(agg_sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            stats_state,
            context,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let agg_sink = GroupedAggregateSink::new(aggregations, group_by, input.schema(), cfg)
                .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(agg_sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Dedup(Dedup {
            input,
            columns,
            schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let dedup_sink = DedupSink::new(columns).with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(dedup_sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let unpivot_op = UnpivotOperator::new(
                ids.clone(),
                values.clone(),
                variable_name.clone(),
                value_name.clone(),
            );
            IntermediateNode::new(
                Arc::new(unpivot_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            pre_agg,
            names,
            schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let pivot_sink = PivotSink::new(
                group_by.clone(),
                pivot_column.clone(),
                value_column.clone(),
                aggregation.clone(),
                names.clone(),
                *pre_agg,
            );
            BlockingSinkNode::new(
                Arc::new(pivot_sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            schema,
            stats_state,
            context,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone(), nulls_first.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            BlockingSinkNode::new(
                Arc::new(sort_sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::TopN(TopN {
            input,
            sort_by,
            descending,
            nulls_first,
            offset,
            limit,
            schema,
            stats_state,
            context,
            ..
        }) => {
            let sink = TopNSink::new(
                sort_by.clone(),
                descending.clone(),
                nulls_first.clone(),
                *limit as usize,
                offset.map(|x| x as usize),
            );
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            BlockingSinkNode::new(
                Arc::new(sink),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            starting_offset,
            schema,
            stats_state,
            context,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let monotonically_increasing_id_sink = MonotonicallyIncreasingIdSink::new(
                column_name.clone(),
                *starting_offset,
                schema.clone(),
            );
            StreamingSinkNode::new(
                Arc::new(monotonically_increasing_id_sink),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            null_equals_null,
            join_type,
            build_on_left,
            schema,
            stats_state,
            context,
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();

            let left_stats_state = left.get_stats_state();
            let right_stats_state = right.get_stats_state();

            // If the build_on_left argument is specified, we use it.
            // Else, to determine whether to use the left or right side of a join for building a probe table, we consider:
            // 1. Cardinality of the sides. Probe tables should be built on the smaller side.
            // 2. Join type. Different join types have different requirements for which side can build the probe table.
            let build_on_left = match build_on_left {
                Some(build_on_left) => *build_on_left,
                None => match join_type {
                    // Inner and outer joins can build on either side. If stats are available, choose the smaller side.
                    // Else, default to building on the left.
                    JoinType::Inner | JoinType::Outer => {
                        match (left_stats_state, right_stats_state) {
                            (
                                StatsState::Materialized(left_stats),
                                StatsState::Materialized(right_stats),
                            ) => {
                                let left_size = left_stats.approx_stats.num_rows;
                                let right_size = right_stats.approx_stats.num_rows;
                                left_size <= right_size
                            }
                            // If stats are only available on the right side of the join, and the upper bound bytes on the
                            // right are under the broadcast join size threshold, we build on the right instead of the left.
                            (
                                StatsState::NotMaterialized,
                                StatsState::Materialized(right_stats),
                            ) => {
                                right_stats.approx_stats.size_bytes
                                    > cfg.broadcast_join_size_bytes_threshold
                            }
                            _ => true,
                        }
                    }
                    // Left joins can build on the left side, but prefer building on the right because building on left requires keeping track
                    // of used indices in a bitmap. If stats are available, only select the left side if its smaller than the right side by a factor of 1.5.
                    JoinType::Left => match (left_stats_state, right_stats_state) {
                        (
                            StatsState::Materialized(left_stats),
                            StatsState::Materialized(right_stats),
                        ) => {
                            let left_size = left_stats.approx_stats.num_rows;
                            let right_size = right_stats.approx_stats.num_rows;
                            right_size as f64 >= left_size as f64 * 1.5
                        }
                        // If stats are only available on the left side of the join, and the upper bound bytes on the left
                        // are under the broadcast join size threshold, we build on the left instead of the right.
                        (StatsState::Materialized(left_stats), StatsState::NotMaterialized) => {
                            left_stats.approx_stats.size_bytes
                                <= cfg.broadcast_join_size_bytes_threshold
                        }
                        _ => false,
                    },
                    // Right joins can build on the right side, but prefer building on the left because building on right requires keeping track
                    // of used indices in a bitmap. If stats are available, only select the right side if its smaller than the left side by a factor of 1.5.
                    JoinType::Right => match (left_stats_state, right_stats_state) {
                        (
                            StatsState::Materialized(left_stats),
                            StatsState::Materialized(right_stats),
                        ) => {
                            let left_size = left_stats.approx_stats.num_rows;
                            let right_size = right_stats.approx_stats.num_rows;
                            (right_size as f64 * 1.5) >= left_size as f64
                        }
                        // If stats are only available on the right side of the join, and the upper bound bytes on the
                        // right are under the broadcast join size threshold, we build on the right instead of the left.
                        (StatsState::NotMaterialized, StatsState::Materialized(right_stats)) => {
                            right_stats.approx_stats.size_bytes
                                > cfg.broadcast_join_size_bytes_threshold
                        }
                        _ => true,
                    },
                    // Anti/semi joins can build on the left side, but prefer building on the right because building on left requires keeping track
                    // of used indices in a bitmap. If stats are available, only select the left side if its smaller than the right side by a factor of 1.5.
                    JoinType::Anti | JoinType::Semi => {
                        match (left_stats_state, right_stats_state) {
                            (
                                StatsState::Materialized(left_stats),
                                StatsState::Materialized(right_stats),
                            ) => {
                                let left_size = left_stats.approx_stats.num_rows;
                                let right_size = right_stats.approx_stats.num_rows;
                                right_size as f64 > left_size as f64 * 1.5
                            }
                            // If stats are only available on the left side of the join, and the upper bound bytes on the left
                            // are under the broadcast join size threshold, we build on the left instead of the right.
                            (StatsState::Materialized(left_stats), StatsState::NotMaterialized) => {
                                left_stats.approx_stats.size_bytes
                                    <= cfg.broadcast_join_size_bytes_threshold
                            }
                            // Else, default to building on the right
                            _ => false,
                        }
                    }
                },
            };
            let (build_on, probe_on, build_child, probe_child) = match build_on_left {
                true => (left_on, right_on, left, right),
                false => (right_on, left_on, right, left),
            };

            let build_schema = build_child.schema();
            let probe_schema = probe_child.schema();
            || -> DaftResult<_> {
                let common_join_cols: IndexSet<_> = get_common_join_cols(left_schema, right_schema)
                    .map(std::string::ToString::to_string)
                    .collect();
                let build_key_fields = build_on
                    .iter()
                    .map(|e| e.inner().to_field(build_schema))
                    .collect::<DaftResult<Vec<_>>>()?;
                let probe_key_fields = probe_on
                    .iter()
                    .map(|e| e.inner().to_field(probe_schema))
                    .collect::<DaftResult<Vec<_>>>()?;

                for (build_field, probe_field) in build_key_fields.iter().zip(probe_key_fields.iter()) {
                    if build_field.dtype != probe_field.dtype {
                        return Err(DaftError::SchemaMismatch(
                            format!("Expected build and probe key field datatypes to match, found: {} vs {}", build_field.dtype, probe_field.dtype)
                        ));
                    }
                }
                let key_schema = Arc::new(Schema::new(build_key_fields));

                // we should move to a builder pattern
                let probe_state_bridge = BroadcastStateBridge::new();
                let track_indices = if matches!(join_type, JoinType::Anti | JoinType::Semi) {
                    build_on_left
                } else {
                    true
                };
                let build_sink = HashJoinBuildSink::new(
                    key_schema,
                    build_on.clone(),
                    null_equals_null.clone(),
                    track_indices,
                    probe_state_bridge.clone(),
                )?;
                let build_child_node = physical_plan_to_pipeline(build_child, psets, cfg, ctx)?;
                let build_node = BlockingSinkNode::new(
                    Arc::new(build_sink),
                    build_child_node,
                    build_child.get_stats_state().clone(),
                    ctx,
                    build_child.schema().clone(),
                    context,
                )
                .boxed();

                let probe_child_node = physical_plan_to_pipeline(probe_child, psets, cfg, ctx)?;

                match join_type {
                    JoinType::Anti | JoinType::Semi => Ok(StreamingSinkNode::new(
                        Arc::new(AntiSemiProbeSink::new(
                            probe_on.clone(),
                            join_type,
                            schema,
                            probe_state_bridge,
                            build_on_left,
                        )),
                        vec![build_node, probe_child_node],
                        stats_state.clone(),
                        ctx,
                        schema.clone(),
                        context,
                    )
                    .boxed()),
                    JoinType::Inner => Ok(IntermediateNode::new(
                        Arc::new(InnerHashJoinProbeOperator::new(
                            probe_on.clone(),
                            left_schema,
                            right_schema,
                            build_on_left,
                            common_join_cols,
                            schema,
                            probe_state_bridge,
                        )),
                        vec![build_node, probe_child_node],
                        stats_state.clone(),
                        ctx,
                        schema.clone(),
                        context,
                    )
                    .boxed()),
                    JoinType::Left | JoinType::Right | JoinType::Outer => {
                        Ok(StreamingSinkNode::new(
                            Arc::new(OuterHashJoinProbeSink::new(
                                probe_on.clone(),
                                left_schema,
                                right_schema,
                                *join_type,
                                build_on_left,
                                common_join_cols,
                                schema,
                                probe_state_bridge,
                            )?),
                            vec![build_node, probe_child_node],
                            stats_state.clone(),
                            ctx,
                            schema.clone(),
                            context,
                        )
                        .boxed())
                    }
                }
            }()
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?
        }
        LocalPhysicalPlan::CrossJoin(CrossJoin {
            left,
            right,
            schema,
            stats_state,
            context,
        }) => {
            let left_stats_state = left.get_stats_state();
            let right_stats_state = right.get_stats_state();

            // To determine whether to use the left or right side of a join for collecting vs streaming, we choose
            // the larger side to stream so that it can be parallelized via an intermediate op. Default to left side.
            let stream_on_left = match (left_stats_state, right_stats_state) {
                (StatsState::Materialized(left_stats), StatsState::Materialized(right_stats)) => {
                    left_stats.approx_stats.num_rows > right_stats.approx_stats.num_rows
                }
                // If stats are only available on the left side of the join, and the upper bound bytes on the
                // left are under the broadcast join size threshold, we stream on the right.
                (StatsState::Materialized(left_stats), StatsState::NotMaterialized) => {
                    left_stats.approx_stats.size_bytes > cfg.broadcast_join_size_bytes_threshold
                }
                // If stats are not available, we fall back and stream on the left by default.
                _ => true,
            };

            let stream_side = if stream_on_left {
                JoinSide::Left
            } else {
                JoinSide::Right
            };

            let (stream_child, collect_child) = match stream_side {
                JoinSide::Left => (left, right),
                JoinSide::Right => (right, left),
            };

            let stream_child_node = physical_plan_to_pipeline(stream_child, psets, cfg, ctx)?;
            let collect_child_node = physical_plan_to_pipeline(collect_child, psets, cfg, ctx)?;

            let state_bridge = BroadcastStateBridge::new();
            let collect_node = BlockingSinkNode::new(
                Arc::new(JoinCollectSink::new(state_bridge.clone())),
                collect_child_node,
                collect_child.get_stats_state().clone(),
                ctx,
                collect_child.schema().clone(),
                context,
            )
            .boxed();

            IntermediateNode::new(
                Arc::new(CrossJoinOperator::new(
                    schema.clone(),
                    stream_side,
                    state_bridge,
                )),
                vec![collect_node, stream_child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::SortMergeJoin(SortMergeJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            schema,
            stats_state,
            context,
        }) => {
            let left_schema = left.schema().clone();
            let left_node = physical_plan_to_pipeline(left, psets, cfg, ctx)?;
            let right_node = physical_plan_to_pipeline(right, psets, cfg, ctx)?;

            let state_bridge = BroadcastStateBridge::new();
            let collect_node = BlockingSinkNode::new(
                Arc::new(JoinCollectSink::new(state_bridge.clone())),
                left_node,
                left.get_stats_state().clone(),
                ctx,
                left.schema().clone(),
                context,
            )
            .boxed();

            StreamingSinkNode::new(
                Arc::new(SortMergeJoinProbe::new(
                    left_on.clone(),
                    right_on.clone(),
                    left_schema,
                    *join_type,
                    state_bridge,
                )),
                vec![collect_node, right_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::PhysicalWrite(PhysicalWrite {
            input,
            file_info,
            file_schema,
            stats_state,
            context,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let writer_factory = make_physical_writer_factory(file_info, input.schema(), cfg)
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;
            let write_format = match (file_info.file_format, file_info.partition_cols.is_some()) {
                (FileFormat::Parquet, true) => WriteFormat::PartitionedParquet,
                (FileFormat::Parquet, false) => WriteFormat::Parquet,
                (FileFormat::Csv, true) => WriteFormat::PartitionedCsv,
                (FileFormat::Csv, false) => WriteFormat::Csv,
                (FileFormat::Json, true) => WriteFormat::PartitionedJson,
                (FileFormat::Json, false) => WriteFormat::Json,
                (_, _) => panic!("Unsupported file format"),
            };
            let write_sink = WriteSink::new(
                write_format,
                writer_factory,
                file_info.partition_cols.clone(),
                file_schema.clone(),
            );
            BlockingSinkNode::new(
                Arc::new(write_sink),
                child_node,
                stats_state.clone(),
                ctx,
                file_schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::CommitWrite(CommitWrite {
            input,
            data_schema,
            file_schema,
            file_info,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let write_sink =
                CommitWriteSink::new(data_schema.clone(), file_schema.clone(), file_info.clone());
            BlockingSinkNode::new(
                Arc::new(write_sink),
                child_node,
                stats_state.clone(),
                ctx,
                file_schema.clone(),
                context,
            )
            .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::CatalogWrite(daft_local_plan::CatalogWrite {
            input,
            catalog_type,
            file_schema,
            stats_state,
            context,
            ..
        }) => {
            use daft_logical_plan::CatalogType;

            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let (partition_by, write_format) = match catalog_type {
                CatalogType::Iceberg(ic) => {
                    if !ic.partition_cols.is_empty() {
                        (
                            Some(ic.partition_cols.clone()),
                            WriteFormat::PartitionedIceberg,
                        )
                    } else {
                        (None, WriteFormat::Iceberg)
                    }
                }
                CatalogType::DeltaLake(dl) => {
                    if let Some(partition_cols) = &dl.partition_cols
                        && !partition_cols.is_empty()
                    {
                        (
                            Some(partition_cols.clone()),
                            WriteFormat::PartitionedDeltalake,
                        )
                    } else {
                        (None, WriteFormat::Deltalake)
                    }
                }
                _ => panic!("Unsupported catalog type"),
            };
            let writer_factory =
                daft_writers::make_catalog_writer_factory(catalog_type, &partition_by, cfg);
            let write_sink = WriteSink::new(
                write_format,
                writer_factory,
                partition_by,
                file_schema.clone(),
            );
            BlockingSinkNode::new(
                Arc::new(write_sink),
                child_node,
                stats_state.clone(),
                ctx,
                file_schema.clone(),
                context,
            )
            .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::LanceWrite(daft_local_plan::LanceWrite {
            input,
            lance_info,
            file_schema,
            stats_state,
            context,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let writer_factory = daft_writers::make_lance_writer_factory(lance_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::Lance,
                writer_factory,
                None,
                file_schema.clone(),
            );
            BlockingSinkNode::new(
                Arc::new(write_sink),
                child_node,
                stats_state.clone(),
                ctx,
                file_schema.clone(),
                context,
            )
            .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::DataSink(daft_local_plan::DataSink {
            input,
            data_sink_info,
            file_schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let writer_factory =
                daft_writers::make_data_sink_writer_factory(data_sink_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::DataSink(data_sink_info.name.clone()),
                writer_factory,
                None,
                file_schema.clone(),
            );
            BlockingSinkNode::new(
                Arc::new(write_sink),
                child_node,
                stats_state.clone(),
                ctx,
                file_schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::Repartition(daft_local_plan::Repartition {
            input,
            repartition_spec,
            num_partitions,
            stats_state,
            schema,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let repartition_op =
                RepartitionSink::new(repartition_spec.clone(), *num_partitions, schema.clone());
            BlockingSinkNode::new(
                Arc::new(repartition_op),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::IntoPartitions(daft_local_plan::IntoPartitions {
            input,
            num_partitions,
            stats_state,
            schema,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let into_partitions_op = IntoPartitionsSink::new(*num_partitions, schema.clone());
            BlockingSinkNode::new(
                Arc::new(into_partitions_op),
                child_node,
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::GlobScan(GlobScan {
            glob_paths,
            pushdowns,
            schema,
            stats_state,
            io_config,
            context,
        }) => {
            use crate::sources::glob_scan::GlobScanSource;
            let source = GlobScanSource::new(
                glob_paths.clone(),
                pushdowns.clone(),
                schema.clone(),
                io_config.clone(),
            );
            SourceNode::new(
                source.arced(),
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
        LocalPhysicalPlan::VLLMProject(VLLMProject {
            input,
            expr,
            llm_actors,
            output_column_name,
            schema,
            stats_state,
            context,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let vllm_sink = VLLMSink::new(
                Arc::new(expr.clone()),
                output_column_name.clone(),
                llm_actors.clone(),
                schema.clone(),
            );
            StreamingSinkNode::new(
                Arc::new(vllm_sink),
                vec![child_node],
                stats_state.clone(),
                ctx,
                schema.clone(),
                context,
            )
            .boxed()
        }
    };

    Ok(pipeline_node)
}
