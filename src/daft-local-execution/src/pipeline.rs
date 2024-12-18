use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_display::{mermaid::MermaidDisplayVisitor, tree::TreeDisplay};
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_core::{
    datatypes::Field,
    join::JoinSide,
    prelude::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::{col, join::get_common_join_keys};
use daft_local_plan::{
    ActorPoolProject, Concat, CrossJoin, EmptyScan, Explode, Filter, HashAggregate, HashJoin,
    InMemoryScan, Limit, LocalPhysicalPlan, MonotonicallyIncreasingId, PhysicalWrite, Pivot,
    Project, Sample, Sort, UnGroupedAggregate, Unpivot,
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
        actor_pool_project::ActorPoolProjectOperator,
        anti_semi_hash_join_probe::AntiSemiProbeOperator, cross_join::CrossJoinOperator,
        explode::ExplodeOperator, filter::FilterOperator,
        inner_hash_join_probe::InnerHashJoinProbeOperator, intermediate_op::IntermediateNode,
        project::ProjectOperator, sample::SampleOperator, unpivot::UnpivotOperator,
    },
    sinks::{
        aggregate::AggregateSink,
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
        write::{WriteFormat, WriteSink},
    },
    sources::{empty_scan::EmptyScanSource, in_memory::InMemorySource},
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

pub fn viz_pipeline(root: &dyn PipelineNode) -> String {
    let mut output = String::new();
    let mut visitor = MermaidDisplayVisitor::new(
        &mut output,
        common_display::DisplayLevel::Default,
        true,
        Default::default(),
    );
    visitor.fmt(root.as_tree_display()).unwrap();
    output
}

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &(impl PartitionSetCache<MicroPartitionRef, Arc<MicroPartitionSet>> + ?Sized),
    cfg: &Arc<DaftExecutionConfig>,
) -> crate::Result<Box<dyn PipelineNode>> {
    use daft_local_plan::PhysicalScan;

    use crate::sources::scan_task::ScanTaskSource;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => {
            let source = EmptyScanSource::new(schema.clone());
            source.arced().into()
        }
        LocalPhysicalPlan::PhysicalScan(PhysicalScan {
            scan_tasks,
            pushdowns,
            schema,
            ..
        }) => {
            let scan_tasks = scan_tasks
                .iter()
                .map(|task| task.clone().as_any_arc().downcast().unwrap())
                .collect::<Vec<ScanTaskRef>>();

            let scan_task_source =
                ScanTaskSource::new(scan_tasks, pushdowns.clone(), schema.clone(), cfg);
            scan_task_source.arced().into()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let cache_key: Arc<str> = info.cache_key.clone().into();

            let materialized_pset = psets
                .get_partition_set(&cache_key)
                .unwrap_or_else(|| panic!("Cache key not found: {:?}", info.cache_key));

            InMemorySource::new(materialized_pset, info.source_schema.clone())
                .arced()
                .into()
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::ActorPoolProject(ActorPoolProject {
            input, projection, ..
        }) => {
            let proj_op = ActorPoolProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
            ..
        }) => {
            let sample_op = SampleOperator::new(*fraction, *with_replacement, *seed);
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(sample_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(filter_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Explode(Explode {
            input, to_explode, ..
        }) => {
            let explode_op = ExplodeOperator::new(to_explode.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            IntermediateNode::new(Arc::new(explode_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            StreamingSinkNode::new(Arc::new(sink), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Concat(Concat { input, other, .. }) => {
            let left_child = physical_plan_to_pipeline(input, psets, cfg)?;
            let right_child = physical_plan_to_pipeline(other, psets, cfg)?;
            let sink = ConcatSink {};
            StreamingSinkNode::new(Arc::new(sink), vec![left_child, right_child]).boxed()
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let agg_sink = AggregateSink::new(aggregations, schema).with_context(|_| {
                PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                }
            })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node).boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let agg_sink = GroupedAggregateSink::new(aggregations, group_by, schema, cfg)
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;
            BlockingSinkNode::new(Arc::new(agg_sink), child_node).boxed()
        }
        LocalPhysicalPlan::Unpivot(Unpivot {
            input,
            ids,
            values,
            variable_name,
            value_name,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let unpivot_op = UnpivotOperator::new(
                ids.clone(),
                values.clone(),
                variable_name.clone(),
                value_name.clone(),
            );
            IntermediateNode::new(Arc::new(unpivot_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Pivot(Pivot {
            input,
            group_by,
            pivot_column,
            value_column,
            aggregation,
            names,
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
            BlockingSinkNode::new(Arc::new(pivot_sink), child_node).boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            nulls_first,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone(), nulls_first.clone());
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            BlockingSinkNode::new(Arc::new(sort_sink), child_node).boxed()
        }
        LocalPhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let monotonically_increasing_id_sink =
                MonotonicallyIncreasingIdSink::new(column_name.clone(), schema.clone());
            StreamingSinkNode::new(Arc::new(monotonically_increasing_id_sink), vec![child_node])
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
                        let left_size = left_stats.approx_stats.upper_bound_bytes;
                        let right_size = right_stats.approx_stats.upper_bound_bytes;
                        left_size.zip(right_size).map_or(true, |(l, r)| l <= r)
                    }
                    // If stats are only available on the right side of the join, and the upper bound bytes on the
                    // right are under the broadcast join size threshold, we build on the right instead of the left.
                    (StatsState::NotMaterialized, StatsState::Materialized(right_stats)) => {
                        right_stats
                            .approx_stats
                            .upper_bound_bytes
                            .map_or(true, |size| size > cfg.broadcast_join_size_bytes_threshold)
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
                        let left_size = left_stats.approx_stats.upper_bound_bytes;
                        let right_size = right_stats.approx_stats.upper_bound_bytes;
                        left_size
                            .zip(right_size)
                            .map_or(false, |(l, r)| (r as f64) >= ((l as f64) * 1.5))
                    }
                    // If stats are only available on the left side of the join, and the upper bound bytes on the left
                    // are under the broadcast join size threshold, we build on the left instead of the right.
                    (StatsState::Materialized(left_stats), StatsState::NotMaterialized) => {
                        left_stats
                            .approx_stats
                            .upper_bound_bytes
                            .map_or(false, |size| {
                                size <= cfg.broadcast_join_size_bytes_threshold
                            })
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
                        let left_size = left_stats.approx_stats.upper_bound_bytes;
                        let right_size = right_stats.approx_stats.upper_bound_bytes;
                        left_size
                            .zip(right_size)
                            .map_or(true, |(l, r)| (r as f64) < ((l as f64) * 1.5))
                    }
                    // If stats are only available on the right side of the join, and the upper bound bytes on the
                    // right are under the broadcast join size threshold, we build on the right instead of the left.
                    (StatsState::NotMaterialized, StatsState::Materialized(right_stats)) => {
                        right_stats
                            .approx_stats
                            .upper_bound_bytes
                            .map_or(false, |size| {
                                size <= cfg.broadcast_join_size_bytes_threshold
                            })
                    }
                    _ => false,
                },

                // Anti and semi joins always build on the right
                JoinType::Anti | JoinType::Semi => false,
            };
            let (build_on, probe_on, build_child, probe_child) = match build_on_left {
                true => (left_on, right_on, left, right),
                false => (right_on, left_on, right, left),
            };

            let build_schema = build_child.schema();
            let probe_schema = probe_child.schema();
            || -> DaftResult<_> {
                let common_join_keys: IndexSet<_> = get_common_join_keys(left_on, right_on)
                    .map(std::string::ToString::to_string)
                    .collect();
                let build_key_fields = build_on
                    .iter()
                    .map(|e| e.to_field(build_schema))
                    .collect::<DaftResult<Vec<_>>>()?;
                let probe_key_fields = probe_on
                    .iter()
                    .map(|e| e.to_field(probe_schema))
                    .collect::<DaftResult<Vec<_>>>()?;
                let key_schema: SchemaRef = Schema::new(
                    build_key_fields
                        .into_iter()
                        .zip(probe_key_fields.into_iter())
                        .map(|(l, r)| {
                            // TODO we should be using the comparison_op function here instead but i'm just using existing behavior for now
                            let dtype = supertype::try_get_supertype(&l.dtype, &r.dtype)?;
                            Ok(Field::new(l.name, dtype))
                        })
                        .collect::<DaftResult<Vec<_>>>()?,
                )?
                .into();

                let casted_build_on = build_on
                    .iter()
                    .zip(key_schema.fields.values())
                    .map(|(e, f)| e.clone().cast(&f.dtype))
                    .collect::<Vec<_>>();
                let casted_probe_on = probe_on
                    .iter()
                    .zip(key_schema.fields.values())
                    .map(|(e, f)| e.clone().cast(&f.dtype))
                    .collect::<Vec<_>>();
                // we should move to a builder pattern
                let probe_state_bridge = BroadcastStateBridge::new();
                let build_sink = HashJoinBuildSink::new(
                    key_schema,
                    casted_build_on,
                    null_equals_null.clone(),
                    join_type,
                    probe_state_bridge.clone(),
                )?;
                let build_child_node = physical_plan_to_pipeline(build_child, psets, cfg)?;
                let build_node =
                    BlockingSinkNode::new(Arc::new(build_sink), build_child_node).boxed();

                let probe_child_node = physical_plan_to_pipeline(probe_child, psets, cfg)?;

                match join_type {
                    JoinType::Anti | JoinType::Semi => Ok(IntermediateNode::new(
                        Arc::new(AntiSemiProbeOperator::new(
                            casted_probe_on,
                            join_type,
                            schema,
                            probe_state_bridge,
                        )),
                        vec![build_node, probe_child_node],
                    )
                    .boxed()),
                    JoinType::Inner => Ok(IntermediateNode::new(
                        Arc::new(InnerHashJoinProbeOperator::new(
                            casted_probe_on,
                            left_schema,
                            right_schema,
                            build_on_left,
                            common_join_keys,
                            schema,
                            probe_state_bridge,
                        )),
                        vec![build_node, probe_child_node],
                    )
                    .boxed()),
                    JoinType::Left | JoinType::Right | JoinType::Outer => {
                        Ok(StreamingSinkNode::new(
                            Arc::new(OuterHashJoinProbeSink::new(
                                casted_probe_on,
                                left_schema,
                                right_schema,
                                *join_type,
                                build_on_left,
                                common_join_keys,
                                schema,
                                probe_state_bridge,
                            )),
                            vec![build_node, probe_child_node],
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
            ..
        }) => {
            let left_stats_state = left.get_stats_state();
            let right_stats_state = right.get_stats_state();

            // To determine whether to use the left or right side of a join for collecting vs streaming, we choose
            // the larger side to stream so that it can be parallelized via an intermediate op. Default to left side.
            let stream_on_left = match (left_stats_state, right_stats_state) {
                (StatsState::Materialized(left_stats), StatsState::Materialized(right_stats)) => {
                    left_stats.approx_stats.upper_bound_bytes
                        > right_stats.approx_stats.upper_bound_bytes
                }
                // If stats are only available on the left side of the join, and the upper bound bytes on the
                // left are under the broadcast join size threshold, we stream on the right.
                (StatsState::Materialized(left_stats), StatsState::NotMaterialized) => left_stats
                    .approx_stats
                    .upper_bound_bytes
                    .map_or(true, |size| size > cfg.broadcast_join_size_bytes_threshold),
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
            )
            .boxed();

            IntermediateNode::new(
                Arc::new(CrossJoinOperator::new(
                    schema.clone(),
                    stream_side,
                    state_bridge,
                )),
                vec![collect_node, stream_child_node],
            )
            .boxed()
        }
        LocalPhysicalPlan::PhysicalWrite(PhysicalWrite {
            input,
            file_info,
            file_schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory = make_physical_writer_factory(file_info, cfg);
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
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node).boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::CatalogWrite(daft_local_plan::CatalogWrite {
            input,
            catalog_type,
            file_schema,
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
                        let partition_col_exprs = partition_cols
                            .iter()
                            .map(|name| col(name.as_str()))
                            .collect::<Vec<_>>();
                        (Some(partition_col_exprs), WriteFormat::PartitionedDeltalake)
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
            BlockingSinkNode::new(Arc::new(write_sink), child_node).boxed()
        }
        #[cfg(feature = "python")]
        LocalPhysicalPlan::LanceWrite(daft_local_plan::LanceWrite {
            input,
            lance_info,
            file_schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory = daft_writers::make_lance_writer_factory(lance_info.clone());
            let write_sink = WriteSink::new(
                WriteFormat::Lance,
                writer_factory,
                None,
                file_schema.clone(),
            );
            BlockingSinkNode::new(Arc::new(write_sink), child_node).boxed()
        }
    };

    Ok(out)
}
