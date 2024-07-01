use std::{collections::HashMap, sync::Arc};

use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{
        Aggregate, Coalesce, FanoutByHash, Filter, HashJoin, InMemoryScan, Limit, Project,
        ReduceMerge, TabularScan,
    },
    PhysicalPlan,
};
use tokio::sync::Mutex;

use crate::{
    intermediate_ops::{
        aggregate::AggregateOperator, fanout::FanoutHashOperator, filter::FilterOperator,
        project::ProjectOperator,
    },
    sinks::{
        coalesce::CoalesceSink, hashjoin::HashJoinSink, limit::LimitSink,
        reducemerge::ReduceMergeSink,
    },
    source::Source,
    streaming_pipeline::StreamingPipeline,
};

pub fn physical_plan_to_streaming_pipeline(
    physical_plan: &Arc<PhysicalPlan>,
    psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
) -> Vec<StreamingPipeline> {
    let mut pipelines = vec![];
    fn recursively_create_pipelines<'a>(
        physical_plan: &Arc<PhysicalPlan>,
        psets: &HashMap<String, Vec<Arc<MicroPartition>>>,
        pipelines: &'a mut Vec<StreamingPipeline>,
    ) -> &'a mut StreamingPipeline {
        match physical_plan.as_ref() {
            PhysicalPlan::InMemoryScan(InMemoryScan { in_memory_info, .. }) => {
                let partitions = psets.get(&in_memory_info.cache_key).unwrap();
                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Data(partitions.clone()), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
                let new_pipeline = StreamingPipeline::new(
                    pipelines.len(),
                    Source::ScanTask(scan_tasks.clone()),
                    None,
                );
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::Aggregate(Aggregate {
                input,
                aggregations,
                groupby,
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let agg_op = AggregateOperator::new(aggregations.clone(), groupby.clone());
                current_pipeline.add_operator(Box::new(agg_op));
                current_pipeline
            }
            PhysicalPlan::Project(Project {
                input, projection, ..
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let proj_op = ProjectOperator::new(projection.clone());
                current_pipeline.add_operator(Box::new(proj_op));
                current_pipeline
            }
            PhysicalPlan::Filter(Filter { input, predicate }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let filter_op = FilterOperator::new(predicate.clone());
                current_pipeline.add_operator(Box::new(filter_op));
                current_pipeline
            }
            PhysicalPlan::Limit(Limit { limit, input, .. }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let sink = LimitSink::new(*limit as usize);
                current_pipeline.set_sink(Arc::new(Mutex::new(sink)));

                let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Arc<MicroPartition>>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::Coalesce(Coalesce {
                input,
                num_from,
                num_to,
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);

                let q = num_from / num_to;
                let r = num_from % num_to;

                let mut distribution = vec![q; *num_to];
                for bucket in distribution.iter_mut().take(r) {
                    *bucket += 1;
                }
                let sink = CoalesceSink::new(distribution);

                current_pipeline.set_sink(Arc::new(Mutex::new(sink)));

                let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Arc<MicroPartition>>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::HashJoin(HashJoin {
                left,
                right,
                left_on,
                right_on,
                join_type,
            }) => {
                {
                    recursively_create_pipelines(left, psets, pipelines);
                };
                let left_idx = pipelines.len() - 1;
                {
                    recursively_create_pipelines(right, psets, pipelines);
                };
                let right_idx = pipelines.len() - 1;
                let hash_join_sink = HashJoinSink::new(
                    left_on.clone(),
                    right_on.clone(),
                    *join_type,
                    left_idx,
                    right_idx,
                );
                let ptr = Arc::new(Mutex::new(hash_join_sink));
                let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Arc<MicroPartition>>>(32);

                pipelines[left_idx].set_sink(ptr.clone());
                pipelines[right_idx].set_sink(ptr);
                pipelines[left_idx].set_next_pipeline_tx(tx.clone());
                pipelines[right_idx].set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            PhysicalPlan::FanoutByHash(FanoutByHash {
                input,
                num_partitions,
                partition_by,
            }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let fanout_hash_op = FanoutHashOperator::new(*num_partitions, partition_by.clone());
                current_pipeline.add_operator(Box::new(fanout_hash_op));
                current_pipeline
            }
            PhysicalPlan::ReduceMerge(ReduceMerge { input }) => {
                let current_pipeline = recursively_create_pipelines(input, psets, pipelines);
                let reduce_merge_sink = ReduceMergeSink::new();
                current_pipeline.set_sink(Arc::new(Mutex::new(reduce_merge_sink)));

                let (tx, rx) = tokio::sync::mpsc::channel::<Vec<Arc<MicroPartition>>>(32);
                current_pipeline.set_next_pipeline_tx(tx);

                let new_pipeline =
                    StreamingPipeline::new(pipelines.len(), Source::Receiver(rx), None);
                pipelines.push(new_pipeline);
                pipelines.last_mut().unwrap()
            }
            _ => {
                unimplemented!(
                    "Physical plan not supported: {:?}",
                    physical_plan.to_string()
                )
            }
        }
    }

    recursively_create_pipelines(physical_plan, psets, &mut pipelines);
    pipelines
}
