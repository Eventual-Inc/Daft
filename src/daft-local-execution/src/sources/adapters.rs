use std::{collections::HashMap, fmt::Debug, sync::Arc};

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_recordbatch::RecordBatch;
use daft_scan::{
    DataSource, DataSourceStatistics, DataSourceTask, DataSourceTaskStatistics, PartitionField,
    Pushdowns, ReadOptions, ScanOperatorRef, ScanTaskRef,
};
use daft_stats::PartitionSpec;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::scan_task::stream_scan_task;

// ── ScanOperatorAdapter ───────────────────────────────────────────────────────

/// Wraps a [`ScanOperatorRef`] as a [`DataSource`].
///
/// Allows all existing [`ScanOperator`] implementations to work through the new
/// trait without individual migrations. Each call to [`get_tasks`] delegates to
/// [`ScanOperator::to_scan_tasks`] and wraps the results as [`ScanTaskAdapter`]s.
#[derive(Debug, Clone)]
pub struct ScanOperatorAdapter(ScanOperatorRef);

impl ScanOperatorAdapter {
    pub fn new(inner: ScanOperatorRef) -> Self {
        Self(inner)
    }
}

impl DataSource for ScanOperatorAdapter {
    fn name(&self) -> String {
        self.0.0.name().to_string()
    }

    fn schema(&self) -> SchemaRef {
        self.0.0.schema()
    }

    fn partition_fields(&self) -> Vec<PartitionField> {
        self.0.0.partitioning_keys().to_vec()
    }

    fn statistics(&self) -> Option<DataSourceStatistics> {
        // ScanOperator has no statistics API; count pushdown is handled separately
        // via supports_count_pushdown() on the legacy path during migration.
        None
    }

    fn get_tasks(&self, pushdowns: &Pushdowns) -> DaftResult<Vec<Arc<dyn DataSourceTask>>> {
        let scan_tasks = self.0.0.to_scan_tasks(pushdowns.clone())?;
        // split() ensures each task has exactly one source, which is required by
        // stream_scan_task (it errors on multi-source tasks).
        Ok(scan_tasks
            .into_iter()
            .flat_map(|st| st.split())
            .map(|st| Arc::new(ScanTaskAdapter::new(st)) as Arc<dyn DataSourceTask>)
            .collect())
    }
}

// ── ScanTaskAdapter ───────────────────────────────────────────────────────────

/// Wraps a [`ScanTaskRef`] as a [`DataSourceTask`].
///
/// Captures the dispatch logic (previously in an external `read_scan_task` match
/// statement) inside the task's [`read`] method. The implementation follows
/// the established Daft pattern: spawn async I/O work onto the IO runtime, bridge
/// results back to the caller through an unbounded channel.
///
/// Iceberg delete map loading is deferred to a follow-up PR; delete files are
/// currently ignored.
#[derive(Debug, Clone)]
pub struct ScanTaskAdapter(ScanTaskRef);

impl ScanTaskAdapter {
    pub fn new(task: ScanTaskRef) -> Self {
        Self(task)
    }
}

#[async_trait::async_trait]
impl DataSourceTask for ScanTaskAdapter {
    fn schema(&self) -> SchemaRef {
        self.0.materialized_schema()
    }

    async fn read(
        &self,
        opts: ReadOptions,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let task = self.0.clone();
        let io_stats = IOStatsRef::default();
        let (tx, rx) = unbounded_channel::<DaftResult<RecordBatch>>();

        // Spawn the async reader onto the IO runtime. Errors and record batches are
        // forwarded through the channel; the stream ends naturally when `tx` is
        // dropped at task completion.
        get_io_runtime(true).spawn(async move {
            // delete_map: None until Iceberg delete support is added here.
            let delete_map: Option<Arc<HashMap<String, Vec<i64>>>> = None;
            match stream_scan_task(
                task,
                io_stats,
                delete_map,
                opts.maintain_order,
                opts.batch_size,
            )
            .await
            {
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
                Ok(mut stream) => {
                    while let Some(result) = stream.next().await {
                        match result {
                            Err(e) => {
                                let _ = tx.send(Err(e));
                                return;
                            }
                            Ok(mp) => {
                                for rb in mp.record_batches().iter().cloned() {
                                    if tx.send(Ok(rb)).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    fn statistics(&self) -> Option<DataSourceTaskStatistics> {
        let size_bytes = match self.0.size_bytes_on_disk {
            Some(b) => daft_scan::Precision::Exact(b),
            None => daft_scan::Precision::Absent,
        };
        let column_stats = self.0.statistics.clone();
        if size_bytes == daft_scan::Precision::Absent && column_stats.is_none() {
            return None;
        }
        Some(DataSourceTaskStatistics {
            size_bytes,
            column_stats,
        })
    }

    fn partition_values(&self) -> Option<&PartitionSpec> {
        self.0.partition_spec()
    }
}
