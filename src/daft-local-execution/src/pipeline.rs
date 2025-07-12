use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{
    ascii::fmt_tree_gitstyle,
    mermaid::{MermaidDisplayVisitor, SubgraphOptions},
    tree::TreeDisplay,
    DisplayLevel,
};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_core::{join::JoinSide, prelude::Schema};
use daft_dsl::join::get_common_join_cols;
use daft_local_plan::{
    ActorPoolProject, CommitWrite, Concat, CrossJoin, Dedup, EmptyScan, Explode, Filter,
    HashAggregate, HashJoin, InMemoryScan, Limit, LocalPhysicalPlan, MonotonicallyIncreasingId,
    PhysicalWrite, Pivot, Project, Sample, Sort, TopN, UnGroupedAggregate, Unpivot,
    WindowOrderByOnly, WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy,
    WindowPartitionOnly,
};
use daft_logical_plan::{stats::StatsState, JoinType};
use daft_micropartition::{
    partitioning::{MicroPartitionSet, PartitionSetCache},
    MicroPartition, MicroPartitionRef,
};
use daft_scan::ScanTaskRef;
use daft_writers::make_physical_writer_factory;
use indexmap::IndexSet;
use snafu::ResultExt;

use crate::{
    channel::Receiver,
    intermediate_ops::{
        actor_pool_project::ActorPoolProjectOperator, cross_join::CrossJoinOperator,
        distributed_actor_pool_project::DistributedActorPoolProjectOperator,
        explode::ExplodeOperator, filter::FilterOperator,
        inner_hash_join_probe::InnerHashJoinProbeOperator, intermediate_op::IntermediateNode,
        project::ProjectOperator, sample::SampleOperator, unpivot::UnpivotOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        blocking_sink::BlockingSinkNode,
        commit_write::CommitWriteSink,
        cross_join_collect::CrossJoinCollectSink,
        dedup::DedupSink,
        grouped_aggregate::GroupedAggregateSink,
        hash_join_build::HashJoinBuildSink,
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
        anti_semi_hash_join_probe::AntiSemiProbeSink, base::StreamingSinkNode, concat::ConcatSink,
        limit::LimitSink, monotonically_increasing_id::MonotonicallyIncreasingIdSink,
        outer_hash_join_probe::OuterHashJoinProbeSink,
    },
    ExecutionRuntimeContext, PipelineCreationSnafu,
};

pub(crate) trait PipelineNode: Sync + Send + TreeDisplay {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    fn name(&self) -> &'static str;
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
}

/// Single use context for translating a physical plan to a Pipeline.
/// It generates a plan_id, and node ids for each plan.
pub struct RuntimeContext {
    index_counter: std::cell::RefCell<usize>,
    context: HashMap<String, String>,
}

/// Contains information about the node such as name, id, and the plan_id
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub name: Arc<str>,
    pub id: usize,
    pub context: HashMap<String, String>,
}

impl RuntimeContext {
    pub fn new() -> Self {
        Self::new_with_context(HashMap::new())
    }

    pub fn new_with_context(mut context: HashMap<String, String>) -> Self {
        if !context.contains_key("plan_id") {
            let plan_id = uuid::Uuid::new_v4().to_string();
            context.insert("plan_id".to_string(), plan_id);
        }

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

    pub fn next_node_info(&self, name: &str) -> NodeInfo {
        NodeInfo {
            name: Arc::from(name.to_string()),
            id: self.next_id(),
            context: self.context.clone(),
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
pub struct RelationshipNode {
    pub id: usize,
    pub parent_id: Option<usize>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", pyo3::pyclass)]
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

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
    cfg: &Arc<DaftExecutionConfig>,
    ctx: &RuntimeContext,
) -> crate::Result<Box<dyn PipelineNode>> {
    use daft_local_plan::PhysicalScan;

    use crate::sources::scan_task::ScanTaskSource;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::PlaceholderScan(_) => {
            panic!("PlaceholderScan should not be converted to a pipeline node")
        }
        LocalPhysicalPlan::EmptyScan(EmptyScan {
            schema,
            stats_state,
        }) => {
            let source = EmptyScanSource::new(schema.clone());
            SourceNode::new(source.arced(), stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            stats_state,
        }) => {
            let scan_tasks = scan_tasks
                .iter()
                .map(|task| task.clone().as_any_arc().downcast().unwrap())
                .collect::<Vec<ScanTaskRef>>();

            let scan_task_source =
                ScanTaskSource::new(scan_tasks, pushdowns.clone(), schema.clone(), cfg);
            SourceNode::new(scan_task_source.arced(), stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::WindowPartitionOnly(WindowPartitionOnly {
            input,
            partition_by,
            schema,
            stats_state,
            aggregations,
            aliases,
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
            )
            .boxed()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, stats_state }) => {
            let cache_key: Arc<str> = info.cache_key.clone().into();

            let materialized_pset = psets.get_partition_set(&cache_key);
            let in_memory_source = InMemorySource::new(
                materialized_pset,
                info.source_schema.clone(),
                info.size_bytes,
            )
            .arced();
            SourceNode::new(in_memory_source, stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::Project(Project {
            input,
            projection,
            stats_state,
            ..
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
            )
            .boxed()
        }
        LocalPhysicalPlan::ActorPoolProject(ActorPoolProject {
            input,
            projection,
            stats_state,
            ..
        }) => {
            let proj_op =
                ActorPoolProjectOperator::try_new(projection.clone()).with_context(|_| {
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
            )
            .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::DistributedActorPoolProject(
            daft_local_plan::DistributedActorPoolProject {
                input,
                actor_objects,
                batch_size,
                memory_request,
                stats_state,
                ..
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
            )
            .boxed()
        }
        LocalPhysicalPlan::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
            stats_state,
            ..
        }) => {
            let sample_op = SampleOperator::new(*fraction, *with_replacement, *seed);
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(sample_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
            )
            .boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input,
            predicate,
            stats_state,
            ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(filter_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
            )
            .boxed()
        }
        LocalPhysicalPlan::Explode(Explode {
            input,
            to_explode,
            stats_state,
            ..
        }) => {
            let explode_op = ExplodeOperator::new(to_explode.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            IntermediateNode::new(
                Arc::new(explode_op),
                vec![child_node],
                stats_state.clone(),
                ctx,
            )
            .boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input,
            num_rows,
            stats_state,
            ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            StreamingSinkNode::new(Arc::new(sink), vec![child_node], stats_state.clone(), ctx)
                .boxed()
        }
        LocalPhysicalPlan::Concat(Concat {
            input,
            other,
            stats_state,
            ..
        }) => {
            let left_child = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let right_child = physical_plan_to_pipeline(other, psets, cfg, ctx)?;
            let sink = ConcatSink {};
            StreamingSinkNode::new(
                Arc::new(sink),
                vec![left_child, right_child],
                stats_state.clone(),
                ctx,
            )
            .boxed()
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let agg_sink = AggregateSink::new(aggregations, input.schema()).with_context(|_| {
                PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                }
            })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node, stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let agg_sink = GroupedAggregateSink::new(aggregations, group_by, input.schema(), cfg)
                .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node, stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::Dedup(Dedup {
            input,
            columns,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let dedup_sink = DedupSink::new(columns).with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(Arc::new(dedup_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        LocalPhysicalPlan::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            stats_state,
            ..
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
            )
            .boxed()
        }
        LocalPhysicalPlan::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let pivot_sink = PivotSink::new(
                group_by.clone(),
                pivot_column.clone(),
                value_column.clone(),
                aggregation.clone(),
                names.clone(),
            );
            BlockingSinkNode::new(Arc::new(pivot_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            stats_state,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone(), nulls_first.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            BlockingSinkNode::new(Arc::new(sort_sink), child_node, stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::TopN(TopN {
            input,
            sort_by,
            descending,
            nulls_first,
            limit,
            stats_state,
            ..
        }) => {
            let sink = TopNSink::new(
                sort_by.clone(),
                descending.clone(),
                nulls_first.clone(),
                *limit as usize,
            );
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            BlockingSinkNode::new(Arc::new(sink), child_node, stats_state.clone(), ctx).boxed()
        }
        LocalPhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            starting_offset,
            schema,
            stats_state,
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
            schema,
            stats_state,
            ..
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();

            // To determine whether to use the left or right side of a join for building a probe table, we consider:
            // 1. Cardinality of the sides. Probe tables should be built on the smaller side.
            // 2. Join type. Different join types have different requirements for which side can build the probe table.
            let left_stats_state = left.get_stats_state();
            let right_stats_state = right.get_stats_state();
            let build_on_left = match join_type {
                // Inner and outer joins can build on either side. If stats are available, choose the smaller side.
                // Else, default to building on the left.
                JoinType::Inner | JoinType::Outer => match (left_stats_state, right_stats_state) {
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
                    (StatsState::NotMaterialized, StatsState::Materialized(right_stats)) => {
                        right_stats.approx_stats.size_bytes
                            > cfg.broadcast_join_size_bytes_threshold
                    }
                    _ => true,
                },
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
                JoinType::Anti | JoinType::Semi => match (left_stats_state, right_stats_state) {
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
                    ctx
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
                        ctx
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
                        ctx
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
                            ctx
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
            ..
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
                Arc::new(CrossJoinCollectSink::new(state_bridge.clone())),
                collect_child_node,
                collect_child.get_stats_state().clone(),
                ctx,
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
            )
            .boxed()
        }
        LocalPhysicalPlan::PhysicalWrite(PhysicalWrite {
            input,
            file_info,
            file_schema,
            stats_state,
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
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        LocalPhysicalPlan::CommitWrite(CommitWrite {
            input,
            file_schema,
            file_info,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let write_sink = CommitWriteSink::new(file_schema.clone(), file_info.clone());
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::CatalogWrite(daft_local_plan::CatalogWrite {
            input,
            catalog_type,
            file_schema,
            stats_state,
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
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::LanceWrite(daft_local_plan::LanceWrite {
            input,
            lance_info,
            file_schema,
            stats_state,
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
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::DataSink(daft_local_plan::DataSink {
            input,
            data_sink_info,
            file_schema,
            stats_state,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let writer_factory =
                daft_writers::make_data_sink_writer_factory(data_sink_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::DataSink,
                writer_factory,
                None,
                file_schema.clone(),
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone(), ctx)
                .boxed()
        }
        LocalPhysicalPlan::Repartition(daft_local_plan::Repartition {
            input,
            columns,
            num_partitions,
            stats_state,
            schema,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg, ctx)?;
            let repartition_op =
                RepartitionSink::new(columns.clone(), *num_partitions, schema.clone());
            BlockingSinkNode::new(
                Arc::new(repartition_op),
                child_node,
                stats_state.clone(),
                ctx,
            )
            .boxed()
        }
    };

    Ok(out)
}
