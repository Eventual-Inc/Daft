#![allow(deprecated, reason = "arrow2 migration")]
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel, tree::TreeDisplay};
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{combine_stream, get_compute_pool_num_threads, get_io_runtime};
use common_scan_info::Pushdowns;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_scan::{ScanTask, ScanTaskRef};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use crate::{
    TaskSet,
    channel::{Receiver, Sender, create_channel},
    pipeline::NodeName,
    plan_input::{InputId, PipelineMessage},
    sources::{
        scan_task::{get_delete_map, stream_scan_task},
        source::{Source, SourceStream},
    },
};

pub struct StreamingScanTaskSource {
    source_id: String,
    receiver: Mutex<Option<Receiver<(InputId, Vec<ScanTaskRef>)>>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    execution_config: Arc<DaftExecutionConfig>,
}

impl StreamingScanTaskSource {
    pub fn new(
        source_id: String,
        receiver: Receiver<(InputId, Vec<ScanTaskRef>)>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        Self {
            source_id,
            receiver: Mutex::new(Some(receiver)),
            pushdowns,
            schema,
            execution_config: Arc::new(cfg.clone()),
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }

    /// Spawns the background task that continuously reads scan tasks from receiver and processes them
    fn spawn_scan_task_processor(
        &self,
        mut receiver: Receiver<(InputId, Vec<ScanTaskRef>)>,
        output_sender: Sender<PipelineMessage>,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> common_runtime::RuntimeTask<DaftResult<()>> {
        let io_runtime = get_io_runtime(true);
        let num_cpus = get_compute_pool_num_threads();
        let source_id = self.source_id.clone();
        io_runtime.spawn(async move {
            let mut task_set = TaskSet::new();
            let mut pending_tasks = Vec::new();
            // Track how many scan tasks are pending per input_id
            // When count reaches 0, we send flush for that input_id
            let input_id_pending_counts: Arc<Mutex<HashMap<InputId, usize>>> = Arc::new(Mutex::new(HashMap::new()));

            loop {
                // Try to fill up to num_cpus parallel tasks
                let max_parallel = num_cpus;

                while task_set.len() < max_parallel {
                    // First, try to get a task from pending_tasks
                    if let Some((scan_task, sender, delete_map, input_id)) = pending_tasks.pop() {
                        task_set.spawn(forward_scan_task_stream(
                            scan_task,
                            io_stats.clone(),
                            delete_map,
                            chunk_size,
                            sender,
                            input_id,
                            source_id.clone(),
                            input_id_pending_counts.clone(),
                        ));
                        continue;
                    }

                    // If no pending tasks, try to receive from channel
                    if let Some((input_id, scan_tasks_batch)) = receiver.recv().await {
                        // Invariant: scan_tasks_batch always has at least one scan task
                        // Compute delete_map for this batch
                        let delete_map = get_delete_map(&scan_tasks_batch).await?.map(Arc::new);

                        // Split all scan tasks for parallelism
                        let split_tasks: Vec<Arc<ScanTask>> = scan_tasks_batch
                            .into_iter()
                            .flat_map(|scan_task| scan_task.split())
                            .collect();

                        // Track how many scan tasks we're processing for this input_id
                        let num_tasks = split_tasks.len();
                        {
                            let mut counts = input_id_pending_counts.lock().unwrap();
                            *counts.entry(input_id).or_insert(0) += num_tasks;
                        }

                        // All tasks from this batch share the same delete_map, output sender, and input_id
                        // forward_scan_task_stream will handle sending empty micropartition if no data
                        for scan_task in split_tasks {
                            pending_tasks.push((
                                scan_task,
                                output_sender.clone(),
                                delete_map.clone(),
                                input_id,
                            ));
                        }
                    } else {
                        // Channel is closed, no more tasks will arrive
                        break;
                    }
                }

                // Wait for at least one task to complete
                if let Some(result) = task_set.join_next().await {
                    result??;
                } else {
                    // No tasks running and no more to start, we're done
                    break;
                }
            }

            // Process any remaining pending tasks
            while let Some((scan_task, sender, delete_map, input_id)) = pending_tasks.pop() {
                task_set.spawn(forward_scan_task_stream(
                    scan_task,
                    io_stats.clone(),
                    delete_map,
                    chunk_size,
                    sender,
                    input_id,
                    source_id.clone(),
                    input_id_pending_counts.clone(),
                ));
            }
            // Wait for all remaining tasks to complete
            while let Some(result) = task_set.join_next().await {
                result??;
            }

            Ok(())
        })
    }
}

#[async_trait]
impl Source for StreamingScanTaskSource {
    #[instrument(name = "StreamingScanTaskSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        // Create output channel for results
        let (output_sender, output_receiver) = create_channel::<PipelineMessage>(1);
        // Spawn a task that continuously reads from self.receiver and forwards to task_sender
        // Receiver implements Clone, so we can clone it for the spawned task
        let receiver_clone = self.receiver.lock().unwrap().take().unwrap();

        // Spawn the scan task processor that continuously reads from task_receiver
        // Delete maps will be computed per batch in the processor
        let processor_task = self.spawn_scan_task_processor(
            receiver_clone,
            output_sender,
            io_stats.clone(),
            chunk_size,
        );

        // Convert receiver to stream
        let result_stream = output_receiver.into_stream().map(Ok);

        // Combine with processor task to handle errors
        let combined_stream = combine_stream(Box::pin(result_stream), processor_task.map(|x| x?));

        Ok(Box::pin(combined_stream))
    }

    fn name(&self) -> NodeName {
        // We can't access scan_tasks here anymore, so use a generic name
        // The actual format will be determined when we receive the tasks
        "StreamingScanTaskSource".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::ScanTask
    }

    fn multiline_display(&self) -> Vec<String> {
        self.display_as(DisplayLevel::Default)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl TreeDisplay for StreamingScanTaskSource {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &StreamingScanTaskSource) -> String {
            format!(
                "StreamingScanTaskSource:
Schema = {}
",
                scan.schema.short_string()
            )
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                let mut s = base_display(self);
                let pushdown = &self.pushdowns;
                if !pushdown.is_empty() {
                    s.push_str(&pushdown.display_as(DisplayLevel::Compact));
                    s.push('\n');
                }

                writeln!(
                    s,
                    "Schema: {{{}}}",
                    self.schema.display_as(DisplayLevel::Compact)
                )
                .unwrap();

                s
            }
            DisplayLevel::Verbose => {
                let mut s = base_display(self);
                writeln!(
                    s,
                    "Pushdowns: {}",
                    self.pushdowns.display_as(DisplayLevel::Verbose)
                )
                .unwrap();
                s
            }
        }
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id(),
            "type": self.op_type().to_string(),
            "name": self.name(),
        })
    }

    fn get_name(&self) -> String {
        "StreamingScanTaskSource".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}

async fn forward_scan_task_stream(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    chunk_size: usize,
    sender: Sender<PipelineMessage>,
    input_id: InputId,
    _source_id: String,
    input_id_pending_counts: Arc<Mutex<HashMap<InputId, usize>>>,
) -> DaftResult<()> {
    use daft_micropartition::MicroPartition;
    let schema = scan_task.materialized_schema();
    let mut stream = stream_scan_task(scan_task, io_stats, delete_map, chunk_size).await?;
    let mut has_data = false;
    while let Some(result) = stream.next().await {
        has_data = true;
        let partition = result?;
        let message = PipelineMessage::Morsel {
            input_id,
            partition,
        };
        if sender.send(message).await.is_err() {
            break;
        }
    }

    // If no data was emitted, send empty micropartition
    if !has_data {
        let empty = Arc::new(MicroPartition::empty(Some(schema)));
        let message = PipelineMessage::Morsel {
            input_id,
            partition: empty,
        };
        let _ = sender.send(message).await;
    }

    // Decrement the count for this input_id and send flush only when all scan tasks for this input_id are done
    let should_send_flush = {
        let mut counts = input_id_pending_counts.lock().unwrap();
        let count = counts.entry(input_id).or_insert(0);
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(&input_id);
            true
        } else {
            false
        }
    };

    if should_send_flush {
        let _ = sender.send(PipelineMessage::Flush(input_id)).await;
    }

    Ok(())
}
