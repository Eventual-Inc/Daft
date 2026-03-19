use std::sync::Arc;

use async_trait::async_trait;
use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel, tree::TreeDisplay};
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::{JoinSet, get_compute_pool_num_threads, get_io_runtime};
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_scan::{DataSource, DataSourceTask, Pushdowns, ReadOptions};
use futures::{FutureExt, StreamExt};
use tracing::instrument;

use crate::{
    channel::{Sender, create_channel},
    pipeline::NodeName,
    sources::source::{Source, SourceStream},
};
use common_runtime::combine_stream;

/// Execution source backed by the [`DataSource`] trait.
///
/// Calls [`DataSource::get_tasks`] to obtain [`DataSourceTask`]s, then spawns
/// parallel readers that call [`DataSourceTask::read`] and wrap the resulting
/// [`RecordBatch`] stream into [`MicroPartition`]s.
pub struct DataSourceSource {
    data_source: Arc<dyn DataSource>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    num_parallel_tasks: usize,
}

impl DataSourceSource {
    pub fn new(
        data_source: Arc<dyn DataSource>,
        pushdowns: Pushdowns,
        schema: SchemaRef,
        cfg: &DaftExecutionConfig,
    ) -> Self {
        let num_cpus = get_compute_pool_num_threads();
        let num_parallel_tasks = if cfg.scantask_max_parallel > 0 {
            cfg.scantask_max_parallel
        } else {
            num_cpus
        };
        Self {
            data_source,
            pushdowns,
            schema,
            num_parallel_tasks,
        }
    }
}

#[async_trait]
impl Source for DataSourceSource {
    #[instrument(name = "DataSourceSource::get_data", level = "info", skip_all)]
    fn get_data(
        &mut self,
        maintain_order: bool,
        _io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>> {
        let tasks = self.data_source.get_tasks(&self.pushdowns)?;
        let schema = self.schema.clone();
        let pushdowns = self.pushdowns.clone();
        let num_parallel = if maintain_order {
            1 // Sequential processing preserves task order.
        } else {
            self.num_parallel_tasks
        };
        let (output_tx, output_rx) = create_channel::<Arc<MicroPartition>>(1);

        let io_runtime = get_io_runtime(true);
        let processor_task = io_runtime.spawn(async move {
            let mut join_set = JoinSet::new();
            let mut task_iter = tasks.into_iter();
            let mut tasks_exhausted = false;

            // Fill initial batch of parallel tasks.
            while join_set.len() < num_parallel && !tasks_exhausted {
                match task_iter.next() {
                    Some(task) => {
                        let sender = output_tx.clone();
                        let task_schema = schema.clone();
                        join_set.spawn(forward_data_source_task(
                            task,
                            sender,
                            task_schema,
                            pushdowns.clone(),
                            maintain_order,
                            chunk_size,
                        ));
                    }
                    None => tasks_exhausted = true,
                }
            }

            loop {
                if join_set.is_empty() && tasks_exhausted {
                    break;
                }

                // Wait for a task to complete, then refill.
                if let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(e.into()),
                    }

                    // Refill slot.
                    if !tasks_exhausted {
                        match task_iter.next() {
                            Some(task) => {
                                let sender = output_tx.clone();
                                let task_schema = schema.clone();
                                join_set.spawn(forward_data_source_task(
                                    task,
                                    sender,
                                    task_schema,
                                    pushdowns.clone(),
                                    maintain_order,
                                    chunk_size,
                                ));
                            }
                            None => tasks_exhausted = true,
                        }
                    }
                }
            }
            Ok(())
        });

        let result_stream = output_rx.into_stream().map(Ok);
        let combined = combine_stream(result_stream, processor_task.map(|x| x?));
        Ok(Box::pin(combined))
    }

    fn name(&self) -> NodeName {
        format!("Read {}(DataSource)", self.data_source.name()).into()
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

impl TreeDisplay for DataSourceSource {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        match level {
            DisplayLevel::Compact => self.get_name(),
            _ => {
                let mut s = format!(
                    "DataSourceSource: {}\nNum Parallel Tasks = {}\n",
                    self.data_source.name(),
                    self.num_parallel_tasks,
                );
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
        "DataSourceSource".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}

/// Read a single [`DataSourceTask`] and forward results as [`MicroPartition`]s.
///
/// Applies post-read filters and limits from pushdowns (the optimizer may have
/// pushed these into the scan node and removed the corresponding plan nodes).
async fn forward_data_source_task(
    task: Arc<dyn DataSourceTask>,
    sender: Sender<Arc<MicroPartition>>,
    schema: SchemaRef,
    pushdowns: Pushdowns,
    maintain_order: bool,
    batch_size: usize,
) -> DaftResult<()> {
    let opts = ReadOptions {
        maintain_order,
        batch_size,
    };

    let mut rows_seen: usize = 0;
    let mut stream = task.read(opts).await?;
    while let Some(result) = stream.next().await {
        // Check limit before processing.
        if let Some(limit) = pushdowns.limit {
            if rows_seen >= limit {
                break;
            }
        }

        let mut record_batch = result?;

        // Apply post-read filter.
        if let Some(ref filter_expr) = pushdowns.filters {
            let bound = BoundExpr::try_new(filter_expr.clone(), &record_batch.schema)?;
            record_batch = record_batch.filter(&[bound])?;
        }

        // Apply limit slicing.
        if let Some(limit) = pushdowns.limit {
            if rows_seen + record_batch.len() > limit {
                record_batch = record_batch.slice(0, limit - rows_seen)?;
            }
            rows_seen += record_batch.len();
        }

        #[allow(deprecated)]
        let record_batch = record_batch.cast_to_schema(schema.as_ref())?;
        let mp = Arc::new(MicroPartition::new_loaded(
            schema.clone(),
            Arc::new(vec![record_batch]),
            None,
        ));
        if sender.send(mp).await.is_err() {
            break;
        }
    }
    Ok(())
}
