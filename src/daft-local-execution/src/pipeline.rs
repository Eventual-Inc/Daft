use std::{collections::HashMap, sync::Arc};

use common_display::{mermaid::MermaidDisplayVisitor, tree::TreeDisplay};
use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    prelude::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::join::get_common_join_keys;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Concat, EmptyScan, Explode, Filter, HashAggregate, HashJoin, InMemoryScan, Limit,
    LocalPhysicalPlan, Pivot, Project, Sample, Sort, UnGroupedAggregate, Unpivot,
};
use daft_plan::JoinType;
use indexmap::IndexSet;
use snafu::ResultExt;

use crate::{
    channel::Receiver,
    intermediate_ops::{
        anti_semi_hash_join_probe::AntiSemiProbeOperator, explode::ExplodeOperator,
        filter::FilterOperator, inner_hash_join_probe::InnerHashJoinProbeOperator,
        intermediate_op::IntermediateNode, pivot::PivotOperator, project::ProjectOperator,
        sample::SampleOperator, unpivot::UnpivotOperator,
    },
    sinks::{
        aggregate::AggregateSink, blocking_sink::BlockingSinkNode, concat::ConcatSink,
        hash_join_build::HashJoinBuildSink, limit::LimitSink,
        outer_hash_join_probe::OuterHashJoinProbeSink, sort::SortSink,
        streaming_sink::StreamingSinkNode,
    },
    sources::{empty_scan::EmptyScanSource, in_memory::InMemorySource},
    ExecutionRuntimeHandle, PipelineCreationSnafu, ProbeStateBridge,
};

pub(crate) trait PipelineNode: Sync + Send + TreeDisplay {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    fn name(&self) -> &'static str;
    fn start(
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>>;

    fn as_tree_display(&self) -> &dyn TreeDisplay;
}

pub(crate) fn viz_pipeline(root: &dyn PipelineNode) -> String {
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

pub(crate) fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> crate::Result<Box<dyn PipelineNode>> {
    use daft_physical_plan::PhysicalScan;

    use crate::sources::scan_task::ScanTaskSource;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => {
            let source = EmptyScanSource::new(schema.clone());
            source.boxed().into()
        }
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            let scan_task_source = ScanTaskSource::new(scan_tasks.clone());
            scan_task_source.boxed().into()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            InMemorySource::new(partitions.clone(), info.source_schema.clone())
                .boxed()
                .into()
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
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
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(sample_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(filter_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Explode(Explode {
            input, to_explode, ..
        }) => {
            let explode_op = ExplodeOperator::new(to_explode.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(explode_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            StreamingSinkNode::new(Arc::new(sink), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Concat(Concat { input, other, .. }) => {
            let left_child = physical_plan_to_pipeline(input, psets)?;
            let right_child = physical_plan_to_pipeline(other, psets)?;
            let sink = ConcatSink {};
            StreamingSinkNode::new(Arc::new(sink), vec![left_child, right_child]).boxed()
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let agg_sink = AggregateSink::new(aggregations, &[], schema);
            BlockingSinkNode::new(Arc::new(agg_sink), child_node).boxed()
        }
        LocalPhysicalPlan::HashAggregate(HashAggregate {
            input,
            aggregations,
            group_by,
            schema,
            ..
        }) => {
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let agg_sink = AggregateSink::new(aggregations, group_by, schema);
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
            let child_node = physical_plan_to_pipeline(input, psets)?;
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
            names,
            ..
        }) => {
            let pivot_op = PivotOperator::new(
                group_by.clone(),
                pivot_column.clone(),
                value_column.clone(),
                names.clone(),
            );
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(pivot_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            BlockingSinkNode::new(Arc::new(sort_sink), child_node).boxed()
        }

        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
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
                    join_type,
                    probe_state_bridge.clone(),
                )?;
                let build_child_node = physical_plan_to_pipeline(build_child, psets)?;
                let build_node =
                    BlockingSinkNode::new(Arc::new(build_sink), build_child_node).boxed();

                let probe_child_node = physical_plan_to_pipeline(probe_child, psets)?;

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
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    };

    Ok(out)
}
