use std::{collections::HashMap, sync::Arc};

use crate::{
    intermediate_ops::{
        aggregate::AggregateOperator, filter::FilterOperator, hash_join_probe::HashJoinProber,
        intermediate_op::IntermediateNode, project::ProjectOperator,
    },
    sinks::{
        aggregate::AggregateSink, blocking_sink::BlockingSinkNode,
        hash_join_build::HashJoinBuildSink, limit::LimitSink, sort::SortSink,
        streaming_sink::StreamingSinkNode,
    },
    sources::in_memory::InMemorySource,
    ExecutionRuntimeHandle,
};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    datatypes::Field,
    schema::{Schema, SchemaRef},
    utils::supertype,
};
use daft_dsl::Expr;
use daft_micropartition::MicroPartition;
use daft_physical_plan::{
    Filter, HashAggregate, HashJoin, InMemoryScan, Limit, LocalPhysicalPlan, Project, Sort,
    UnGroupedAggregate,
};
use daft_plan::populate_aggregation_stages;

use crate::channel::MultiSender;

#[async_trait]
pub trait PipelineNode: Sync + Send {
    fn children(&self) -> Vec<&dyn PipelineNode>;
    async fn start(
        &mut self,
        destination: MultiSender,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> DaftResult<()>;
}

pub fn physical_plan_to_pipeline(
    physical_plan: &LocalPhysicalPlan,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> DaftResult<Box<dyn PipelineNode>> {
    use crate::sources::scan_task::ScanTaskSource;
    use daft_physical_plan::PhysicalScan;
    let out: Box<dyn PipelineNode> = match physical_plan {
        LocalPhysicalPlan::PhysicalScan(PhysicalScan { scan_tasks, .. }) => {
            let scan_task_source = ScanTaskSource::new(scan_tasks.clone());
            scan_task_source.boxed().into()
        }
        LocalPhysicalPlan::InMemoryScan(InMemoryScan { info, .. }) => {
            let partitions = psets.get(&info.cache_key).expect("Cache key not found");
            InMemorySource::new(partitions.clone()).boxed().into()
        }
        LocalPhysicalPlan::Project(Project {
            input, projection, ..
        }) => {
            let proj_op = ProjectOperator::new(projection.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(proj_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Filter(Filter {
            input, predicate, ..
        }) => {
            let filter_op = FilterOperator::new(predicate.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            IntermediateNode::new(Arc::new(filter_op), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Limit(Limit {
            input, num_rows, ..
        }) => {
            let sink = LimitSink::new(*num_rows as usize);
            let child_node = physical_plan_to_pipeline(input, psets)?;
            StreamingSinkNode::new(sink.boxed(), vec![child_node]).boxed()
        }
        LocalPhysicalPlan::Concat(_) => {
            todo!("concat")
            // let sink = ConcatSink::new();
            // let left_child = physical_plan_to_pipeline(input, psets)?;
            // let right_child = physical_plan_to_pipeline(other, psets)?;
            // PipelineNode::double_sink(sink, left_child, right_child)
        }
        LocalPhysicalPlan::UnGroupedAggregate(UnGroupedAggregate {
            input,
            aggregations,
            schema,
            ..
        }) => {
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, &[]);
            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let post_first_agg_node =
                IntermediateNode::new(Arc::new(first_stage_agg_op), vec![child_node]).boxed();

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                vec![],
            );
            let second_stage_node =
                BlockingSinkNode::new(second_stage_agg_sink.boxed(), post_first_agg_node).boxed();

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
            let (first_stage_aggs, second_stage_aggs, final_exprs) =
                populate_aggregation_stages(aggregations, schema, group_by);
            let first_stage_agg_op = AggregateOperator::new(
                first_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
            );
            let child_node = physical_plan_to_pipeline(input, psets)?;
            let post_first_agg_node =
                IntermediateNode::new(Arc::new(first_stage_agg_op), vec![child_node]).boxed();

            let second_stage_agg_sink = AggregateSink::new(
                second_stage_aggs
                    .values()
                    .cloned()
                    .map(|e| Arc::new(Expr::Agg(e.clone())))
                    .collect(),
                group_by.clone(),
            );
            let second_stage_node =
                BlockingSinkNode::new(second_stage_agg_sink.boxed(), post_first_agg_node).boxed();

            let final_stage_project = ProjectOperator::new(final_exprs);

            IntermediateNode::new(Arc::new(final_stage_project), vec![second_stage_node]).boxed()
        }
        LocalPhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            ..
        }) => {
            let sort_sink = SortSink::new(sort_by.clone(), descending.clone());
            let child_node = physical_plan_to_pipeline(input, psets)?;
            BlockingSinkNode::new(sort_sink.boxed(), child_node).boxed()
        }
        LocalPhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            ..
        }) => {
            let left_schema = left.schema();
            let right_schema = right.schema();

            let left_key_fields = left_on
                .iter()
                .map(|e| e.to_field(left_schema))
                .collect::<DaftResult<Vec<_>>>()?;
            let right_key_fields = right_on
                .iter()
                .map(|e| e.to_field(right_schema))
                .collect::<DaftResult<Vec<_>>>()?;

            let key_schema: SchemaRef = Schema::new(
                left_key_fields
                    .into_iter()
                    .zip(right_key_fields.into_iter())
                    .map(|(l, r)| {
                        // TODO we should be using the comparison_op function here instead but i'm just using existing behavior for now
                        let dtype = supertype::try_get_supertype(&l.dtype, &r.dtype)?;
                        Ok(Field::new(l.name, dtype))
                    })
                    .collect::<DaftResult<Vec<_>>>()?,
            )?
            .into();

            let casted_left_on = left_on
                .iter()
                .zip(key_schema.fields.values())
                .map(|(e, f)| e.clone().cast(&f.dtype))
                .collect::<Vec<_>>();
            let casted_right_on = right_on
                .iter()
                .zip(key_schema.fields.values())
                .map(|(e, f)| e.clone().cast(&f.dtype))
                .collect::<Vec<_>>();

            let left_node = physical_plan_to_pipeline(left, psets)?;
            let right_node = physical_plan_to_pipeline(right, psets)?;

            // we should move to a builder pattern
            let build_sink =
                HashJoinBuildSink::new(casted_left_on.clone(), *join_type, &key_schema)?;
            let build_sink_node = BlockingSinkNode::new(build_sink.boxed(), left_node).boxed();

            let probe_op = HashJoinProber::new(casted_right_on);
            IntermediateNode::new(Arc::new(probe_op), vec![build_sink_node, right_node]).boxed()
        }
        _ => {
            unimplemented!("Physical plan not supported: {}", physical_plan.name());
        }
    };

    Ok(out)
}
