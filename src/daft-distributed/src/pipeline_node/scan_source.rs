use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, ClusteringSpec};
use daft_schema::schema::SchemaRef;

use super::{
    DistributedPipelineNode, NodeName, PipelineNodeConfig, PipelineNodeContext,
    SubmittableTaskStream,
};
use crate::{
    pipeline_node::{make_new_task_from_materialized_outputs, NodeID},
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
        batch_size: u64,
    ) -> DaftResult<()> {
        if batch_size == 0 {
            return Err(
                DaftError::InternalError("Batch size must be greater than 0".to_string()).into(),
            );
        }
        let batch_size = match batch_size.try_into() {
            Ok(bs) => bs,
            Err(e) => {
                return Err(DaftError::InternalError(format!(
                "Batch size {batch_size} is too large to convert to usize on this platform: {e:?}"
            ))
                .into())
            }
        };

        // Step 1: Materialize the scan task to get ray data pointers
        let submit_single_scan_task = SubmittableTask::new(self.make_source_tasks(
            vec![single_scan_task].into(),
            TaskContext::from((&self.context, task_id_counter.next())),
        )?);

        let maybe_materialized_output = submit_single_scan_task.submit(&scheduler_handle)?.await?;

        if let Some(materialized_output) = maybe_materialized_output {
            // Step 2: Split the materialized output into batches of size 'k'
            let materialized_outputs = materialized_output.split_into_materialized_outputs();

            // Step 3: Create into_batches tasks for each batch
            for batch in materialized_outputs.chunks(batch_size) {
                let batch_materialized_outputs = batch.to_vec();

                let task = make_new_task_from_materialized_outputs(
                    TaskContext::from((&self.context, task_id_counter.next())),
                    batch_materialized_outputs,
                    &(self.clone() as Arc<dyn DistributedPipelineNode>),
                    move |input| {
                        LocalPhysicalPlan::into_batches(
                            input,
                            batch_size,
                            true, // Strict batch sizes
                            StatsState::NotMaterialized,
                        )
                    },
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
