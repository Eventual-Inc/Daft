use std::{collections::HashMap, sync::Arc};

use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{
        Aggregate, BroadcastJoin, Coalesce, Concat, EmptyScan, Explode, FanoutByHash, FanoutRandom,
        Filter, Flatten, HashJoin, InMemoryScan, Limit, MonotonicallyIncreasingId, Project,
        ReduceMerge, Sample, Sort, SortMergeJoin, Split, TabularScan, TabularWriteCsv,
        TabularWriteJson, TabularWriteParquet,
    },
    InMemoryInfo, OutputFileInfo, PhysicalPlan,
};

use crate::{
    executor::Executor,
    ops::{
        filter::FilterOp,
        join::HashJoinOp,
        limit::LimitOp,
        monotonically_increasing_id::MonotonicallyIncreasingIdOp,
        project::ProjectOp,
        scan::ScanOp,
        shuffle::{FanoutHashOp, FanoutRandomOp, ReduceMergeOp},
        sort::{BoundarySamplingOp, FanoutRangeOp, SamplesToQuantilesOp, SortedMergeOp},
        FusedOpBuilder, PartitionTaskOp,
    },
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    stage::{
        exchange::{collect::CollectExchange, sort::SortExchange, Exchange, ShuffleExchange},
        sink::{collect::CollectSinkSpec, limit::LimitSinkSpec, SinkSpec},
        Stage,
    },
    tree::{OpNode, PartitionTaskNodeBuilder},
};

#[cfg(feature = "python")]
use daft_plan::physical_ops::{DeltaLakeWrite, IcebergWrite, LanceWrite};

// struct PhysicalPlanToPartitionTaskTreeVisitor<'a, T: PartitionRef> {
//     leaf_inputs: Vec<VirtualPartitionSet<T>>,
//     psets: &'a HashMap<String, Vec<T>>,
// }

// impl<'a, T: PartitionRef> TreeNodeVisitor for PhysicalPlanToPartitionTaskTreeVisitor<'a, T> {
//     type Node = Arc<PhysicalPlan>;

//     fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
//         Ok(TreeNodeRecursion::Continue)
//     }

//     fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
//         match node.as_ref() {
//             _ => todo!(),
//         }
//         Ok(TreeNodeRecursion::Continue)
//     }
// }

fn physical_plan_to_partition_task_tree<T: PartitionRef>(
    physical_plan: &PhysicalPlan,
    leaf_inputs: &mut Vec<VirtualPartitionSet<T>>,
    psets: &HashMap<String, Vec<T>>,
) -> PartitionTaskNodeBuilder {
    let node_type = physical_plan.name();

    let todo_string = format!("physical_plan_to_partition_task_tree: {node_type}");
    match physical_plan {
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => {
            leaf_inputs.push(VirtualPartitionSet::PartitionRef(psets[cache_key].clone()));
            PartitionTaskNodeBuilder::LeafMemory(None)
        }
        PhysicalPlan::TabularScan(TabularScan { scan_tasks, .. }) => {
            leaf_inputs.push(VirtualPartitionSet::ScanTask(scan_tasks.clone()));
            let scan_op = ScanOp::new();
            let op_builder = FusedOpBuilder::new(Arc::new(scan_op));
            PartitionTaskNodeBuilder::LeafScan(op_builder)
        }
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!("{todo_string}"),
        PhysicalPlan::Project(Project {
            input,
            projection,
            resource_request,
            ..
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let project_op = Arc::new(ProjectOp::new(projection.clone(), resource_request.clone()));
            builder.fuse_or_link(project_op)
        }
        PhysicalPlan::Filter(Filter { input, predicate }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let filter_op = Arc::new(FilterOp::new(vec![predicate.clone()]));
            builder.fuse_or_link(filter_op)
        }
        PhysicalPlan::Limit(Limit {
            input,
            limit,
            eager,
            num_partitions,
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let limit_op = Arc::new(LimitOp::new(*limit as usize));
            builder.fuse_or_link(limit_op)
        }
        PhysicalPlan::Explode(Explode {
            input, to_explode, ..
        }) => todo!("{todo_string}"),
        PhysicalPlan::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
        }) => todo!("{todo_string}"),
        PhysicalPlan::MonotonicallyIncreasingId(MonotonicallyIncreasingId {
            input,
            column_name,
        }) => {
            let upstream_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            // Manually insert pipeline boundary for monotonically increasing ID.
            let upstream_node = upstream_builder.build();
            let mon_op = Arc::new(MonotonicallyIncreasingIdOp::new(column_name.clone()));
            let op_builder = FusedOpBuilder::new(mon_op);
            PartitionTaskNodeBuilder::Inner(vec![upstream_node], op_builder)
        }
        PhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            num_partitions,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Split(Split {
            input,
            input_num_partitions,
            output_num_partitions,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Flatten(Flatten { input }) => todo!("{todo_string}"),
        PhysicalPlan::FanoutRandom(FanoutRandom {
            input,
            num_partitions,
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let fanout_random_op = Arc::new(FanoutRandomOp::new(*num_partitions));
            builder.fuse_or_link(fanout_random_op)
        }
        PhysicalPlan::FanoutByHash(FanoutByHash {
            input,
            num_partitions,
            partition_by,
        }) => {
            let builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), leaf_inputs, psets);
            let fanout_hash_op = Arc::new(FanoutHashOp::new(*num_partitions, partition_by.clone()));
            builder.fuse_or_link(fanout_hash_op)
        }
        PhysicalPlan::FanoutByRange(_) => unimplemented!(
            "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
        ),
        PhysicalPlan::ReduceMerge(ReduceMerge { input }) => todo!("{todo_string}"),
        PhysicalPlan::Aggregate(Aggregate {
            aggregations,
            groupby,
            input,
            ..
        }) => todo!("{todo_string}"),
        PhysicalPlan::Coalesce(Coalesce {
            input,
            num_from,
            num_to,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Concat(Concat { other, input }) => todo!("{todo_string}"),
        PhysicalPlan::HashJoin(HashJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            ..
        }) => {
            let left_node =
                physical_plan_to_partition_task_tree::<T>(left.as_ref(), leaf_inputs, psets)
                    .build();
            let right_node =
                physical_plan_to_partition_task_tree::<T>(right.as_ref(), leaf_inputs, psets)
                    .build();
            let join_op = Arc::new(HashJoinOp::new(
                left_on.clone(),
                right_on.clone(),
                *join_type,
            ));
            let op_builder = FusedOpBuilder::new(join_op);
            PartitionTaskNodeBuilder::Inner(vec![left_node, right_node], op_builder)
        }
        PhysicalPlan::SortMergeJoin(SortMergeJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left_is_larger,
            needs_presort,
        }) => todo!("{todo_string}"),
        PhysicalPlan::BroadcastJoin(BroadcastJoin {
            broadcaster: left,
            receiver: right,
            left_on,
            right_on,
            join_type,
            is_swapped,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteJson(TabularWriteJson {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Pivot(_) => todo!("{todo_string}"),
        PhysicalPlan::Unpivot(_) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::IcebergWrite(IcebergWrite {
            schema: _,
            iceberg_info,
            input,
        }) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::DeltaLakeWrite(DeltaLakeWrite {
            schema: _,
            delta_lake_info,
            input,
        }) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::LanceWrite(LanceWrite {
            schema: _,
            lance_info,
            input,
        }) => todo!("{todo_string}"),
    }
}

pub fn physical_plan_to_stage<T: PartitionRef, E: Executor<T> + 'static>(
    physical_plan: &PhysicalPlan,
    is_streaming: bool,
    psets: &HashMap<String, Vec<T>>,
    executor: Arc<E>,
) -> Stage<T, E> {
    let node_type = physical_plan.name();

    let todo_string = format!("physical_plan_to_stage: {node_type}");

    match physical_plan {
        PhysicalPlan::TabularScan(_)
        | PhysicalPlan::Project(_)
        | PhysicalPlan::Filter(_)
        | PhysicalPlan::HashJoin(_)
        | PhysicalPlan::MonotonicallyIncreasingId(_) => {
            // TODO(Clark): Abstract out the following common pattern into a visitor:
            //   1. DFS post-order traversal to create a task graph for child while also gathering inputs in same order.
            //   2. Create exchange/sink on current node configuration, with child task graph as an input.
            //   3. Return exchange/sink + gathered inputs.
            let mut leaf_inputs = vec![];
            let task_graph =
                physical_plan_to_partition_task_tree::<T>(physical_plan, &mut leaf_inputs, psets)
                    .build();
            if is_streaming {
                let sink: Box<dyn SinkSpec<T, E> + Send> =
                    Box::new(CollectSinkSpec::new(task_graph));
                (sink, leaf_inputs).into()
            } else {
                let exchange: Box<dyn Exchange<T>> =
                    Box::new(CollectExchange::new(task_graph, executor));
                (exchange, leaf_inputs).into()
            }
        }
        PhysicalPlan::InMemoryScan(InMemoryScan {
            in_memory_info: InMemoryInfo { cache_key, .. },
            ..
        }) => todo!("{todo_string}"),
        PhysicalPlan::EmptyScan(EmptyScan { schema, .. }) => todo!("{todo_string}"),
        PhysicalPlan::Limit(Limit {
            input,
            limit,
            eager,
            num_partitions,
        }) => {
            let mut leaf_inputs = vec![];
            let task_graph =
                physical_plan_to_partition_task_tree::<T>(physical_plan, &mut leaf_inputs, psets)
                    .build();
            let sink: Box<dyn SinkSpec<T, E> + Send> =
                Box::new(LimitSinkSpec::new(task_graph, *limit as usize));
            (sink, leaf_inputs).into()
        }
        PhysicalPlan::Explode(Explode {
            input, to_explode, ..
        }) => todo!("{todo_string}"),
        PhysicalPlan::Sample(Sample {
            input,
            fraction,
            with_replacement,
            seed,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Sort(Sort {
            input,
            sort_by,
            descending,
            num_partitions,
        }) => {
            let mut leaf_inputs = vec![];
            let upstream_task_tree_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), &mut leaf_inputs, psets);
            let upstream_task_graph = upstream_task_tree_builder.build();

            let sampling_task_op = BoundarySamplingOp::new(*num_partitions, sort_by.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(sampling_task_op);
            let sampling_task_graph = OpNode::LeafMemory(Some(task_op).into());

            let reduce_to_quantiles_op = SamplesToQuantilesOp::new(
                *num_partitions,
                sort_by.clone(),
                descending.clone(),
                sampling_task_graph.num_outputs(),
            );
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(reduce_to_quantiles_op);
            let reduce_to_quantiles_task_graph = OpNode::LeafMemory(Some(task_op).into());

            let fanout_range_op =
                FanoutRangeOp::new(*num_partitions, sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(fanout_range_op);
            let map_task_graph = OpNode::LeafMemory(Some(task_op).into());

            let sorted_merge_op =
                SortedMergeOp::new(*num_partitions, sort_by.clone(), descending.clone());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(sorted_merge_op);
            let reduce_task_graph = OpNode::LeafMemory(Some(task_op).into());

            let sort_exchange: Box<dyn Exchange<T>> = Box::new(SortExchange::new(
                upstream_task_graph,
                sampling_task_graph,
                reduce_to_quantiles_task_graph,
                map_task_graph,
                reduce_task_graph,
                executor,
            ));
            (sort_exchange, leaf_inputs).into()
        }
        PhysicalPlan::Split(Split {
            input,
            input_num_partitions,
            output_num_partitions,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Flatten(Flatten { input }) => todo!("{todo_string}"),
        PhysicalPlan::FanoutRandom(FanoutRandom {
            input,
            num_partitions,
        }) => todo!("{todo_string}"),
        PhysicalPlan::FanoutByHash(FanoutByHash {
            input,
            num_partitions,
            partition_by,
        }) => todo!("{todo_string}"),
        PhysicalPlan::FanoutByRange(_) => unimplemented!(
            "FanoutByRange not implemented, since only use case (sorting) doesn't need it yet."
        ),
        PhysicalPlan::ReduceMerge(ReduceMerge { input }) => {
            let mut leaf_inputs = vec![];
            let map_task_tree_builder =
                physical_plan_to_partition_task_tree::<T>(input.as_ref(), &mut leaf_inputs, psets);
            let map_task_graph = map_task_tree_builder.build();

            let reduce_merge_op = ReduceMergeOp::new(map_task_graph.num_outputs());
            let task_op: Arc<dyn PartitionTaskOp<Input = MicroPartition>> =
                Arc::new(reduce_merge_op);
            let reduce_task_graph = OpNode::LeafMemory(Some(task_op).into());

            let shuffle_exchange: Box<dyn Exchange<T>> = Box::new(ShuffleExchange::new(
                map_task_graph,
                reduce_task_graph,
                executor,
            ));
            (shuffle_exchange, leaf_inputs).into()
        }
        PhysicalPlan::Aggregate(Aggregate {
            aggregations,
            groupby,
            input,
            ..
        }) => todo!("{todo_string}"),
        PhysicalPlan::Coalesce(Coalesce {
            input,
            num_from,
            num_to,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Concat(Concat { other, input }) => todo!("{todo_string}"),
        PhysicalPlan::SortMergeJoin(SortMergeJoin {
            left,
            right,
            left_on,
            right_on,
            join_type,
            num_partitions,
            left_is_larger,
            needs_presort,
        }) => todo!("{todo_string}"),
        PhysicalPlan::BroadcastJoin(BroadcastJoin {
            broadcaster: left,
            receiver: right,
            left_on,
            right_on,
            join_type,
            is_swapped,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteParquet(TabularWriteParquet {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteCsv(TabularWriteCsv {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::TabularWriteJson(TabularWriteJson {
            schema,
            file_info:
                OutputFileInfo {
                    root_dir,
                    file_format,
                    partition_cols,
                    compression,
                    io_config,
                },
            input,
        }) => todo!("{todo_string}"),
        PhysicalPlan::Pivot(_) => todo!("{todo_string}"),
        PhysicalPlan::Unpivot(_) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::IcebergWrite(IcebergWrite {
            schema: _,
            iceberg_info,
            input,
        }) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::DeltaLakeWrite(DeltaLakeWrite {
            schema: _,
            delta_lake_info,
            input,
        }) => todo!("{todo_string}"),
        #[cfg(feature = "python")]
        PhysicalPlan::LanceWrite(LanceWrite {
            schema: _,
            lance_info,
            input,
        }) => todo!("{todo_string}"),
    }
}
