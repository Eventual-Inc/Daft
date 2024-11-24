use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{mermaid::MermaidDisplayVisitor, tree::TreeDisplay};
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_core::{
    datatypes::Field,
    prelude::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::{col, join::get_common_join_keys, Expr};
use daft_local_plan::{
    ActorPoolProject, Concat, EmptyScan, Explode, Filter, HashAggregate, HashJoin, InMemoryScan,
    Limit, LocalPhysicalPlan, MonotonicallyIncreasingId, PhysicalWrite, Pivot, Project, Sample,
    Sort, UnGroupedAggregate, Unpivot,
};
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{extract_agg_expr, populate_aggregation_stages};
use daft_scan::ScanTaskRef;
use daft_writers::make_physical_writer_factory;
use indexmap::IndexSet;
use snafu::ResultExt;

use crate::{
    channel::Receiver,
    intermediate_ops::{
        actor_pool_project::ActorPoolProjectOperator, aggregate::AggregateOperator,
        anti_semi_hash_join_probe::AntiSemiProbeOperator, explode::ExplodeOperator,
        filter::FilterOperator, inner_hash_join_probe::InnerHashJoinProbeOperator,
        intermediate_op::IntermediateNode, project::ProjectOperator, sample::SampleOperator,
        unpivot::UnpivotOperator,
    },
    sinks::{
        aggregate::AggregateSink,
        blocking_sink::BlockingSinkNode,
        concat::ConcatSink,
        hash_join_build::{HashJoinBuildSink, ProbeStateBridge},
        limit::LimitSink,
        monotonically_increasing_id::MonotonicallyIncreasingIdSink,
        outer_hash_join_probe::OuterHashJoinProbeSink,
        pivot::PivotSink,
        sort::SortSink,
        streaming_sink::StreamingSinkNode,
        write::{WriteFormat, WriteSink},
    },
    sources::{empty_scan::EmptyScanSource, in_memory::InMemorySource},
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
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
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
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            InMemorySource::new(partitions.clone(), info.source_schema.clone())
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
            let aggregations = aggregations
                .iter()
                .map(extract_agg_expr)
                .collect::<DaftResult<Vec<_>>>()
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;

            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(&aggregations, schema, &[]);
            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                vec![],
            );
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let post_first_agg_node =
                IntermediateNode::new(Arc::new(first_stage_agg_op), vec![child_node]).boxed();

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                vec![],
            );
            let second_stage_node =
                BlockingSinkNode::new(Arc::new(second_stage_agg_sink), post_first_agg_node).boxed();

            let final_stage_project = ProjectOperator::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project), vec![second_stage_node]).boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            ..
        }) => {
            let aggregations = aggregations
                .iter()
                .map(extract_agg_expr)
                .collect::<DaftResult<Vec<_>>>()
                .with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;

            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(&aggregations, schema, group_by);
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let (post_first_agg_node, group_by) = if !first_stage_aggs.is_empty() {
                let agg_op = AggregateOperator::new(
                    first_stage_aggs
                        .values()
                        .cloned()
                        .map(|e| Arc::new(Expr::Agg(e)))
                        .collect(),
                    group_by.clone(),
                );
                (
                    IntermediateNode::new(Arc::new(agg_op), vec![child_node]).boxed(),
                    &group_by.iter().map(|e| col(e.name())).collect(),
                )
            } else {
                (child_node, group_by)
            };

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                group_by.clone(),
            );
            let second_stage_node =
                BlockingSinkNode::new(Arc::new(second_stage_agg_sink), post_first_agg_node).boxed();

            let final_stage_project = ProjectOperator::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project), vec![second_stage_node]).boxed()
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
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();

            // Determine the build and probe sides based on the join type
            // Currently it is a naive determination, in the future we should leverage the cardinality of the tables
            // to determine the build and probe sides
            let build_on_left = match join_type {
                JoinType::Inner => true,
                JoinType::Right => true,
                JoinType::Outer => true,
                JoinType::Left => false,
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
                let probe_state_bridge = ProbeStateBridge::new();
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
        LocalPhysicalPlan::PhysicalWrite(PhysicalWrite {
            input,
            file_info,
            data_schema,
            file_schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets, cfg)?;
            let writer_factory = make_physical_writer_factory(file_info, data_schema, cfg);
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
            data_schema,
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
            let writer_factory = daft_writers::make_catalog_writer_factory(
                catalog_type,
                data_schema,
                &partition_by,
                cfg,
            );
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
