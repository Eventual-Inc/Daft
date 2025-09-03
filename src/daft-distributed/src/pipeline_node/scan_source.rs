use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{TreeNode, TreeNodeVisitor};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, ClusteringSpec};
use daft_schema::schema::SchemaRef;

use super::{
    DistributedPipelineNode, NodeName, PipelineNodeConfig, PipelineNodeContext,
    SubmittableTaskStream,
};
use crate::{
    pipeline_node::{
        append_plan_to_existing_task, make_in_memory_task_from_materialized_outputs,
        make_new_task_from_materialized_outputs, udf, NodeID,
    },
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask},
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
};

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    is_map_only_pipeline: bool,
}

impl ScanSourceNode {
    const NODE_NAME: NodeName = "ScanSource";

    pub fn new(
        node_id: NodeID,
        stage_config: &StageConfig,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        schema: SchemaRef,
        logical_node_id: Option<NodeID>,
        is_map_only_pipeline: bool,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![],
            vec![],
            logical_node_id,
        );
        let config = PipelineNodeConfig::new(
            schema,
            stage_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(
                scan_tasks.len(),
            )),
        );
        Self {
            config,
            context,
            pushdowns,
            scan_tasks,
            is_map_only_pipeline,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        if self.scan_tasks.is_empty() {
            let empty_scan_task = self
                .make_empty_scan_task(TaskContext::from((&self.context, task_id_counter.next())))?;
            let _ = result_tx.send(SubmittableTask::new(empty_scan_task)).await;
            return Ok(());
        }

        for scan_task in self.scan_tasks.iter() {
            let task = self.make_source_tasks(
                vec![scan_task.clone()].into(),
                TaskContext::from((&self.context, task_id_counter.next())),
            )?;
            if result_tx.send(SubmittableTask::new(task)).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn execute_optimized_scan(
        self: Arc<Self>,
        single_scan_task: ScanTaskLikeRef,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        default_batch_size: usize,
    ) -> DaftResult<()> {
        // Step 1: walk the plan and get the minimum batch size declared by any operator

        let submittable_task_scan = SubmittableTask::new(self.make_source_tasks(
            vec![single_scan_task].into(),
            TaskContext::from((&self.context, task_id_counter.next())),
        )?);

        let batch_size = min_batch_size_in_task_plan(submittable_task_scan.task().plan())
            .unwrap_or(default_batch_size);
        if batch_size == 0 {
            return Err(
                DaftError::InternalError("Batch size must be greater than 0".to_string()).into(),
            );
        }

        // Step 2: Materialize the scan task to get ray data pointers

        let scan_and_into_batches_task = append_plan_to_existing_task(
            submittable_task_scan,
            &(self.clone() as Arc<dyn DistributedPipelineNode>),
            &(move |input| {
                LocalPhysicalPlan::into_batches(
                    input,
                    batch_size,
                    true, // Strict batch sizes
                    StatsState::NotMaterialized,
                )
            }),
        );
        // we created a task with 2 operations:
        //      (1) physical_scan
        //      (2) into_batches

        let maybe_materialized_output = scan_and_into_batches_task
            .submit(&scheduler_handle)?
            .await?;

        //
        //     a single scan task generally corresponds to a single partition
        //
        //     could have multiple scan tasks in one file
        //         e.g. parquet with row groups (constant time to offset into a row group)
        //
        // --- have ---
        //
        // 1. create scan task
        // 2. materialize scan task
        // 3. generate into batches task
        // 4. "return" this
        //
        // --- want ---
        //
        // 1. create scan task
        // 2. create into batches task and **append** to scan task
        // 3. >> we cannot just *return* <<
        //
        // we need to inform the scheduler that it can move these "batches" around freely
        // for the scheduler to move them, it must be aware of them <-- workers need to send information back to the scheduler
        //

        if let Some(materialized_output) = maybe_materialized_output {
            // Step 3: Split the materialized output into batches of size 'k'

            let materialized_outputs = materialized_output.split_into_materialized_outputs();

            // Step 4: Create in-memory tasks for each batch

            for batch in materialized_outputs.chunks(batch_size) {
                let batch_materialized_outputs = batch.to_vec();

                let task = make_in_memory_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    batch_materialized_outputs,
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    None,
                )?;

                match result_tx.send(task).await {
                    Ok(()) => (),
                    Err(e) => {
                        return Err(DaftError::InternalError(format!(
                            "Failed to send internal batch to result channel: {e:?}"
                        ))
                        .into());
                    }
                }
            }
        }

        Ok(())
    }

    fn make_source_tasks(
        &self,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        task_context: TaskContext,
    ) -> DaftResult<SwordfishTask> {
        let physical_scan = LocalPhysicalPlan::physical_scan(
            scan_tasks.clone(),
            self.pushdowns.clone(),
            self.config.schema.clone(),
            StatsState::NotMaterialized,
        );

        let task = SwordfishTask::new(
            task_context,
            physical_scan,
            self.config.execution_config.clone(),
            Default::default(),
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }

    fn make_empty_scan_task(&self, task_context: TaskContext) -> DaftResult<SwordfishTask> {
        let transformed_plan = LocalPhysicalPlan::empty_scan(self.config.schema.clone());
        let psets = HashMap::new();
        let task = SwordfishTask::new(
            task_context,
            transformed_plan,
            self.config.execution_config.clone(),
            psets,
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for ScanSourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![]
    }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let (result_tx, result_rx) = create_channel(1);

        // Check if this is a map-only pipeline by examining the stage type
        // (this is performed at construction time from the LogicalPlanRef)
        // And make sure that we only have 1 scan task
        if self.is_map_only_pipeline && self.scan_tasks.len() == 1 {
            let self_clone = self.clone();
            let task_id_counter = stage_context.task_id_counter();
            let scheduler_handle = stage_context.scheduler_handle();
            let execution_future = async move {
                self_clone
                    .execute_optimized_scan(
                        self.scan_tasks[0].clone(),
                        task_id_counter,
                        result_tx,
                        scheduler_handle,
                        self.config.execution_config.suggested_batch_size,
                    )
                    .await
            };
            stage_context.spawn(execution_future);
        } else {
            // Fall back to normal execution
            let execution_loop = self.execution_loop(result_tx, stage_context.task_id_counter());
            stage_context.spawn(execution_loop);
        }

        SubmittableTaskStream::from(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for ScanSourceNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &ScanSourceNode) -> String {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| st.size_bytes_on_disk().unwrap_or(0))
                .sum();

            #[allow(unused_mut)]
            let mut s = format!(
                "ScanTaskSource:
Num Scan Tasks = {num_scan_tasks}
Estimated Scan Bytes = {total_bytes}
"
            );
            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config().as_ref()
            {
                if num_scan_tasks == 1 {
                    writeln!(s, "SQL Query = {}", &config.sql).unwrap();
                } else {
                    writeln!(s, "SQL Queries = [{},..]", &config.sql).unwrap();
                }
            }
            s
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                let mut s = base_display(self);
                // We're only going to display the pushdowns and schema for the first scan task.
                let pushdown = self.scan_tasks[0].pushdowns();
                if !pushdown.is_empty() {
                    s.push_str(&pushdown.display_as(DisplayLevel::Compact));
                    s.push('\n');
                }

                let schema = self.scan_tasks[0].schema();
                writeln!(
                    s,
                    "Schema: {{{}}}",
                    schema.display_as(DisplayLevel::Compact)
                )
                .unwrap();

                let tasks = self.scan_tasks.iter();

                writeln!(s, "Scan Tasks: [").unwrap();
                for (i, st) in tasks.enumerate() {
                    if i < 3 || i >= self.scan_tasks.len() - 3 {
                        writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Compact)).unwrap();
                    } else if i == 3 {
                        writeln!(s, "...").unwrap();
                    }
                }
                writeln!(s, "]").unwrap();

                s
            }
            DisplayLevel::Verbose => {
                let mut s = base_display(self);
                writeln!(s, "Scan Tasks: [").unwrap();

                for st in self.scan_tasks.iter() {
                    writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Verbose)).unwrap();
                }
                s
            }
        }
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}

/// Looks at all nodes in the plan and gets the minimuim specified batch size.
pub fn min_batch_size_in_task_plan(plan: Arc<LocalPhysicalPlan>) -> Option<usize> {
    let mut find_batch_size = DetermineBatchSize {
        bs: None,
        is_min: true,
    };
    &plan.visit(&mut find_batch_size);
    find_batch_size.bs
}

struct DetermineBatchSize {
    bs: Option<usize>,
    is_min: bool,
}

impl DetermineBatchSize {
    fn visit_and_update(&mut self, node: &daft_local_plan::LocalPhysicalPlanRef) {
        let maybe_observed_batch_size = match &**node {
            // does have a batch_size
            daft_local_plan::LocalPhysicalPlan::UDFProject(udf_project) => {
                udf_project.udf_properties().batch_size
            }
            daft_local_plan::LocalPhysicalPlan::IntoBatches(into_batches) => {
                Some(into_batches.batch_size)
            }
            #[cfg(feature = "python")]
            daft_local_plan::LocalPhysicalPlan::DistributedActorPoolProject(dist_pool) => {
                dist_pool.batch_size
            }

            // does not have a batch_size
            daft_local_plan::LocalPhysicalPlan::InMemoryScan(_)
            | daft_local_plan::LocalPhysicalPlan::PhysicalScan(_)
            | daft_local_plan::LocalPhysicalPlan::EmptyScan(_)
            | daft_local_plan::LocalPhysicalPlan::PlaceholderScan(_)
            | daft_local_plan::LocalPhysicalPlan::Project(_)
            | daft_local_plan::LocalPhysicalPlan::Filter(_)
            | daft_local_plan::LocalPhysicalPlan::Limit(_)
            | daft_local_plan::LocalPhysicalPlan::Explode(_)
            | daft_local_plan::LocalPhysicalPlan::Unpivot(_)
            | daft_local_plan::LocalPhysicalPlan::Sort(_)
            | daft_local_plan::LocalPhysicalPlan::TopN(_)
            | daft_local_plan::LocalPhysicalPlan::Sample(_)
            | daft_local_plan::LocalPhysicalPlan::MonotonicallyIncreasingId(_)
            | daft_local_plan::LocalPhysicalPlan::UnGroupedAggregate(_)
            | daft_local_plan::LocalPhysicalPlan::HashAggregate(_)
            | daft_local_plan::LocalPhysicalPlan::Dedup(_)
            | daft_local_plan::LocalPhysicalPlan::Pivot(_)
            | daft_local_plan::LocalPhysicalPlan::Concat(_)
            | daft_local_plan::LocalPhysicalPlan::HashJoin(_)
            | daft_local_plan::LocalPhysicalPlan::CrossJoin(_)
            | daft_local_plan::LocalPhysicalPlan::PhysicalWrite(_)
            | daft_local_plan::LocalPhysicalPlan::CommitWrite(_)
            | daft_local_plan::LocalPhysicalPlan::WindowPartitionOnly(_)
            | daft_local_plan::LocalPhysicalPlan::WindowPartitionAndOrderBy(_)
            | daft_local_plan::LocalPhysicalPlan::WindowPartitionAndDynamicFrame(_)
            | daft_local_plan::LocalPhysicalPlan::WindowOrderByOnly(_)
            | daft_local_plan::LocalPhysicalPlan::Repartition(_)
            | daft_local_plan::LocalPhysicalPlan::IntoPartitions(_) => None,
            #[cfg(feature = "python")]
            daft_local_plan::LocalPhysicalPlan::CatalogWrite(_)
            | daft_local_plan::LocalPhysicalPlan::LanceWrite(_)
            | daft_local_plan::LocalPhysicalPlan::DataSink(_) => None,
        };

        if let Some(new_bs) = maybe_observed_batch_size {
            match self.bs {
                Some(existing_bs) => {
                    if (self.is_min && new_bs < existing_bs)
                        || (!self.is_min && new_bs > existing_bs)
                    {
                        self.bs = Some(new_bs);
                    }
                }
                None => self.bs = Some(new_bs),
            }
        }
    }
}

impl TreeNodeVisitor for DetermineBatchSize {
    type Node = daft_local_plan::LocalPhysicalPlanRef;

    /// Perform this action after we've visited this node's children. (Bottom-up)
    fn f_down(&mut self, node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        self.visit_and_update(node);
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }

    // Perform this action right before visiting the node's chidlren. (Top-down)
    fn f_up(&mut self, node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        self.visit_and_update(node);
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_daft_config::DaftExecutionConfig;
    use daft_dsl::{
        expr::bound_expr::BoundExpr,
        functions::{
            map::MapExpr,
            python::{LegacyPythonUDF, MaybeInitializedUDF, RuntimePyObject},
            FunctionExpr,
        },
    };
    use daft_local_plan::LocalPhysicalPlan;
    use daft_schema::prelude::{DataType, Field, Schema};

    use crate::utils::channel::create_unbounded_channel;

    use super::*;

    fn fake_udf_fn_expr(batch_size: Option<usize>) -> daft_dsl::expr::Expr {
        daft_dsl::expr::Expr::Function {
            func: FunctionExpr::Python(LegacyPythonUDF {
                name: Arc::new("dummy".to_string()),
                func: MaybeInitializedUDF::Uninitialized {
                    inner: RuntimePyObject::new_none(),
                    init_args: RuntimePyObject::new_none(),
                },
                bound_args: RuntimePyObject::new_none(),
                num_expressions: 1,
                return_dtype: DataType::Boolean,
                resource_request: None,
                batch_size: batch_size,
                concurrency: None,
                use_process: None,
            }),
            inputs: vec![],
        }
    }

    #[test]
    fn test_min_batch_size_in_task_plan() {
        // Test with a plan that has 3 stages: EmptyScan -> IntoBatches -> UDFProject
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int64)]));

        // Stage 1: EmptyScan
        let stage1 = LocalPhysicalPlan::empty_scan(schema.clone());

        // Stage 2: IntoBatches with batch_size 200
        let stage2 =
            LocalPhysicalPlan::into_batches(stage1, 200, true, StatsState::NotMaterialized);

        // Stage 3: UDFProject (no batch_size in physical plan)
        let plan = LocalPhysicalPlan::udf_project(
            stage2,
            BoundExpr::try_new(fake_udf_fn_expr(Some(250)), &schema).unwrap(), // convert to BoundExpr
            vec![],                                                        // no passthrough columns
            schema,
            StatsState::NotMaterialized,
        );

        let result = min_batch_size_in_task_plan(plan);
        assert_eq!(result, Some(200)); // Should return the batch size from IntoBatches
    }

    #[test]
    fn test_min_batch_size_no_batch_size() {
        // Test with a plan that has 3 stages but no batch size: EmptyScan -> Project -> Filter
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int64)]));

        // Stage 1: EmptyScan
        let stage1 = LocalPhysicalPlan::empty_scan(schema.clone());

        // Stage 2: Project (no batch size)
        let stage2 = LocalPhysicalPlan::project(
            stage1,
            vec![], // empty projection
            schema.clone(),
            StatsState::NotMaterialized,
        );

        // Stage 3: Filter (no batch size)
        let plan = LocalPhysicalPlan::filter(
            stage2,
            BoundExpr::try_new(daft_dsl::lit(true), &schema).unwrap(), // convert to BoundExpr
            StatsState::NotMaterialized,
        );

        let result = min_batch_size_in_task_plan(plan);
        assert_eq!(result, None); // Should return None since no operators have batch_size
    }

    #[test]
    fn test_auto_into_batches() {
        // TODO: construct a single scan task from an in-memory source
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Int64)]));
        
        // Create an in-memory scan task
        let in_memory_info = daft_logical_plan::source_info::InMemoryInfo {
            num_partitions: 1,
            size_bytes: 1000,
            num_rows: 100,
            source_schema: schema.clone(),
            cache_key: "".to_string(),
            cache_entry: None,
            clustering_spec: None,
            source_stage_id: None,
        };
        
        let scan_task = LocalPhysicalPlan::in_memory_scan(
            in_memory_info,
            StatsState::NotMaterialized,
        );
        
        // Create a ScanSourceNode with is_map_only_pipeline = true
        let stage_config = StageConfig::new(
            0,
            0,
            Arc::new(DaftExecutionConfig::default()),
        );
        
        let scan_source_node = ScanSourceNode::new(
            0, // node_id
            &stage_config,
            Pushdowns::default(),
            Arc::new(vec![scan_task]),
            schema,
            Some(0), // logical_node_id
            true, // is_map_only_pipeline = true to trigger optimized scan
        );
        
        let (scheduler_sender, scheduler_receiver) = create_unbounded_channel();
        // TODO: call produce_tasks on this
        let mut stage_context = StageExecutionContext::new(
            SchedulerHandle::new(scheduler_sender)
        );
        
        let task_stream = scan_source_node.arced().produce_tasks(&mut stage_context);
        
        // TODO: inspect the result and ensure that it has a new IntoBatches node
        // Note: This is a simplified test - in practice you'd need to:
        // 1. Mock the scheduler handle properly
        // 2. Handle the async nature of the task stream
        // 3. Inspect the actual tasks produced
        
        // For now, we just verify that the task stream was created successfully
        // SubmittableTaskStream doesn't have is_some(), so we just verify it exists
        // The actual verification would involve:
        // - Collecting tasks from the stream
        // - Checking that each task has an IntoBatches node in its plan
        // - Verifying the batch size is set correctly
    }
}
