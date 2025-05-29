use std::sync::Arc;

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
    ActorPoolProject, Concat, CrossJoin, EmptyScan, Explode, Filter, HashAggregate, HashJoin,
    InMemoryScan, Limit, LocalPhysicalPlan, MonotonicallyIncreasingId, PhysicalWrite, Pivot,
    Project, Sample, Sort, TopN, UnGroupedAggregate, Unpivot, WindowOrderByOnly,
    WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy, WindowPartitionOnly,
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
        explode::ExplodeOperator, filter::FilterOperator,
        inner_hash_join_probe::InnerHashJoinProbeOperator, intermediate_op::IntermediateNode,
        project::ProjectOperator, sample::SampleOperator, unpivot::UnpivotOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        anti_semi_hash_join_probe::AntiSemiProbeSink,
        blocking_sink::BlockingSinkNode,
        concat::ConcatSink,
        cross_join_collect::CrossJoinCollectSink,
        grouped_aggregate::GroupedAggregateSink,
        hash_join_build::HashJoinBuildSink,
        limit::LimitSink,
        monotonically_increasing_id::MonotonicallyIncreasingIdSink,
        outer_hash_join_probe::OuterHashJoinProbeSink,
        pivot::PivotSink,
        sort::SortSink,
        streaming_sink::StreamingSinkNode,
        top_n::TopNSink,
        window_order_by_only::WindowOrderByOnlySink,
        window_partition_and_dynamic_frame::WindowPartitionAndDynamicFrameSink,
        window_partition_and_order_by::WindowPartitionAndOrderBySink,
        window_partition_only::WindowPartitionOnlySink,
        write::{WriteFormat, WriteSink},
    },
    sources::{empty_scan::EmptyScanSource, in_memory::InMemorySource, source::SourceNode},
    state_bridge::BroadcastStateBridge,
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
            SourceNode::new(source.arced(), stats_state.clone()).boxed()
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
            SourceNode::new(scan_task_source.arced(), stats_state.clone()).boxed()
        }
        LocalPhysicalPlan::WindowPartitionOnly(WindowPartitionOnly {
            input,
            partition_by,
            schema,
            stats_state,
            aggregations,
            aliases,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let window_partition_only_sink =
                WindowPartitionOnlySink::new(aggregations, aliases, partition_by, schema)
                    .with_context(|_| PipelineCreationSnafu {
                        plan_name: physical_plan.name(),
                    })?;
            BlockingSinkNode::new(
                Arc::new(window_partition_only_sink),
                input_node,
                stats_state.clone(),
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowPartitionAndOrderBy(WindowPartitionAndOrderBy {
            input,
            partition_by,
            order_by,
            descending,
            schema,
            stats_state,
            functions,
            aliases,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let window_partition_and_order_by_sink = WindowPartitionAndOrderBySink::new(
                functions,
                aliases,
                partition_by,
                order_by,
                descending,
                schema,
            )
            .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(
                Arc::new(window_partition_and_order_by_sink),
                input_node,
                stats_state.clone(),
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowPartitionAndDynamicFrame(WindowPartitionAndDynamicFrame {
            input,
            partition_by,
            order_by,
            descending,
            frame,
            min_periods,
            schema,
            stats_state,
            aggregations,
            aliases,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let window_partition_and_dynamic_frame_sink = WindowPartitionAndDynamicFrameSink::new(
                aggregations,
                *min_periods,
                aliases,
                partition_by,
                order_by,
                descending,
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
            )
            .boxed()
        }
        LocalPhysicalPlan::WindowOrderByOnly(WindowOrderByOnly {
            input,
            order_by,
            descending,
            schema,
            stats_state,
            functions,
            aliases,
        }) => {
            let input_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let window_order_by_only_op =
                WindowOrderByOnlySink::new(functions, aliases, order_by, descending, schema)
                    .with_context(|_| PipelineCreationSnafu {
                        plan_name: physical_plan.name(),
                    })?;
            BlockingSinkNode::new(
                Arc::new(window_order_by_only_op),
                input_node,
                stats_state.clone(),
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
            SourceNode::new(in_memory_source, stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node], stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node], stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(sample_op), vec![child_node], stats_state.clone())
                .boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input,
            predicate,
            stats_state,
            ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(filter_op), vec![child_node], stats_state.clone())
                .boxed()
        }
        LocalPhysicalPlan::Explode(Explode {
            input,
            to_explode,
            stats_state,
            ..
        }) => {
            let explode_op = ExplodeOperator::new(to_explode.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(explode_op), vec![child_node], stats_state.clone())
                .boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input,
            num_rows,
            stats_state,
            ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            StreamingSinkNode::new(Arc::new(sink), vec![child_node], stats_state.clone()).boxed()
        }
        LocalPhysicalPlan::Concat(Concat {
            input,
            other,
            stats_state,
            ..
        }) => {
            let left_child = physical_plan_to_pipeline(input, psets, cfg)?;
            let right_child = physical_plan_to_pipeline(other, psets, cfg)?;
            let sink = ConcatSink {};
            StreamingSinkNode::new(
                Arc::new(sink),
                vec![left_child, right_child],
                stats_state.clone(),
            )
            .boxed()
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let agg_sink = AggregateSink::new(aggregations, input.schema()).with_context(|_| {
                PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                }
            })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node, stats_state.clone()).boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let agg_sink = GroupedAggregateSink::new(aggregations, group_by, input.schema(), cfg)
                .with_context(|_| PipelineCreationSnafu {
                plan_name: physical_plan.name(),
            })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node, stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let unpivot_op = UnpivotOperator::new(
                ids.clone(),
                values.clone(),
                variable_name.clone(),
                value_name.clone(),
            );
            IntermediateNode::new(Arc::new(unpivot_op), vec![child_node], stats_state.clone())
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let pivot_sink = PivotSink::new(
                group_by.clone(),
                pivot_column.clone(),
                value_column.clone(),
                aggregation.clone(),
                names.clone(),
            );
            BlockingSinkNode::new(Arc::new(pivot_sink), child_node, stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            BlockingSinkNode::new(Arc::new(sort_sink), child_node, stats_state.clone()).boxed()
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            BlockingSinkNode::new(Arc::new(sink), child_node, stats_state.clone()).boxed()
        }
        LocalPhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            schema,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let monotonically_increasing_id_sink =
                MonotonicallyIncreasingIdSink::new(column_name.clone(), schema.clone());
            StreamingSinkNode::new(
                Arc::new(monotonically_increasing_id_sink),
                vec![child_node],
                stats_state.clone(),
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
                let build_child_node = physical_plan_to_pipeline(build_child, psets, cfg)?;
                let build_node = BlockingSinkNode::new(
                    Arc::new(build_sink),
                    build_child_node,
                    build_child.get_stats_state().clone(),
                )
                .boxed();

                let probe_child_node = physical_plan_to_pipeline(probe_child, psets, cfg)?;

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

            let stream_child_node = physical_plan_to_pipeline(stream_child, psets, cfg)?;
            let collect_child_node = physical_plan_to_pipeline(collect_child, psets, cfg)?;

            let state_bridge = BroadcastStateBridge::new();
            let collect_node = BlockingSinkNode::new(
                Arc::new(CrossJoinCollectSink::new(state_bridge.clone())),
                collect_child_node,
                collect_child.get_stats_state().clone(),
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
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory = make_physical_writer_factory(file_info, input.schema(), cfg)
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;
            let write_format = match (file_info.file_format, file_info.partition_cols.is_some()) {
                (FileFormat::Parquet, true) => WriteFormat::PartitionedParquet,
                (FileFormat::Parquet, false) => WriteFormat::Parquet,
                (FileFormat::Csv, true) => WriteFormat::PartitionedCsv,
                (FileFormat::Csv, false) => WriteFormat::Csv,
                (_, _) => panic!("Unsupported file format"),
            };
            let write_sink = WriteSink::new(
                write_format,
                writer_factory,
                file_info.partition_cols.clone(),
                file_schema.clone(),
                Some(file_info.clone()),
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone()).boxed()
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

            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
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
                None,
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone()).boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::LanceWrite(daft_local_plan::LanceWrite {
            input,
            lance_info,
            file_schema,
            stats_state,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory = daft_writers::make_lance_writer_factory(lance_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::Lance,
                writer_factory,
                None,
                file_schema.clone(),
                None,
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone()).boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::DataSink(daft_local_plan::DataSink {
            input,
            data_sink_info,
            file_schema,
            stats_state,
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory =
                daft_writers::make_data_sink_writer_factory(data_sink_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::DataSink,
                writer_factory,
                None,
                file_schema.clone(),
                None,
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node, stats_state.clone()).boxed()
        }
    };

    Ok(out)
}
