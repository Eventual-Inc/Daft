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
    stage::{self, StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, create_oneshot_channel, Sender},
};

pub(crate) struct ScanSourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pushdowns: Pushdowns,
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
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
        batch_size: usize,
    ) -> DaftResult<()> {
        if batch_size == 0 {
            return Err(
                DaftError::InternalError("Batch size must be greater than 0".to_string()).into(),
            );
        }

        // Step 1: Materialize the scan task to get ray data pointers
        // let scan_task = SubmittableTask::new(SwordfishTask::new(
        //     TaskContext::from((&self.context, task_id_counter.next())),
        //     // self.scan_tasks[0].clone(), // Use the single scan task
        //     single_scan_task.clone(),
        //     self.config.execution_config.clone(),
        //     HashMap::from([(self.node_id().to_string(), vec![single_scan_task])]),
        //     SchedulingStrategy::Spread,
        //     self.context.to_hashmap(),
        // ));

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
                    Ok(_) => (),
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

    // fn produce_tasks(
    //     self: Arc<Self>,
    //     stage_context: &mut StageExecutionContext,
    // ) -> SubmittableTaskStream {
    //     let (result_tx, result_rx) = create_channel(1);

    //     if self.scan_tasks.len() == 1 {
    //         // materialize the read -> send to scheduler
    //         let task_read = SubmittableTask::new(SwordfishTask::new(...));
    //         // task_read.submit(&stage_context.scheduler_handle());
    //         let read_result = stage_context.scheduler_handle().submit_task(task_read);
    //         match read_result {
    //             Ok(o) => match o {
    //                 Some(materialized_output) => {}
    //                 None => {
    //                     let (err_result_tx, err_result_rx) = create_oneshot_channel();
    //                     // err_result_tx.send()
    //                     // TODO [mg] can we send the error task?
    //                     // what do we do here???
    //                     return SubmittableTaskStream::from(err_result_rx);
    //                 }
    //             },
    //             Err(_) => {
    //                 panic!("TODO [mg] what do we do here? *HOW* do we send the error?");
    //             }
    //         }

    //         // get the result --> generate N batches of size K each
    //         stage_context.spawn(task_in_batches);
    //         // TODO [mg] this needs to be another scheduler send

    //         // we now have lots of these data pointers, each is to one of these InBatches
    //         // --> we can now send down the line a ton of In-memory read Tasks

    //         stage_context.spawn(tasks_from_in_batches);
    //         // TODO [mg] THIS ONE IS OK TO .spawn()
    //         // TODO [mg] but, how do we create the right tasks? there should be an absolute
    //         // ton of tasks from the in-batches thing
    //     }

    //     let execution_loop = self.execution_loop(result_tx, stage_context.task_id_counter());
    //     stage_context.spawn(execution_loop);

    //     SubmittableTaskStream::from(result_rx)
    // }

    fn produce_tasks(
        self: Arc<Self>,
        stage_context: &mut StageExecutionContext,
    ) -> SubmittableTaskStream {
        let (result_tx, result_rx) = create_channel(1);

        // Check if this is a map-only pipeline by examining the stage type
        // And make sure that we only have 1 scan task
        if self.scan_tasks.len() == 1 {
            let batch_size = 1000;
            let self_clone = self.clone();
            let task_id_counter = stage_context.task_id_counter().clone();
            let scheduler_handle = stage_context.scheduler_handle().clone();
            let execution_future = async move {
                self_clone
                    .execute_optimized_scan(
                        self.scan_tasks[0].clone(),
                        task_id_counter,
                        result_tx,
                        scheduler_handle,
                        batch_size,
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
