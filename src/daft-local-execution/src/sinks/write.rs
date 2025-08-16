use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{snapshot, Stat, StatSnapshotSend};
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::{AsyncFileWriter, WriterFactory};
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{
    dispatcher::{DispatchSpawner, PartitionedDispatcher, UnorderedDispatcher},
    ops::NodeType,
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::{RuntimeStats, CPU_US_KEY, ROWS_EMITTED_KEY, ROWS_RECEIVED_KEY},
    ExecutionTaskSpawner,
};

#[derive(Default)]
struct WriteStats {
    cpu_us: AtomicU64,
    rows_received: AtomicU64,
    rows_emitted: AtomicU64, // TODO: Remove or rename to files written?
    bytes_written: AtomicU64,
}

impl RuntimeStats for WriteStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_RECEIVED_KEY; Stat::Count(self.rows_received.load(ordering)),
            ROWS_EMITTED_KEY; Stat::Count(self.rows_emitted.load(ordering)),
            "bytes written"; Stat::Bytes(self.bytes_written.load(ordering)),
        ]
    }

    fn add_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_emitted(&self, rows: u64) {
        self.rows_emitted.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub enum WriteFormat {
    Parquet,
    PartitionedParquet,
    Csv,
    PartitionedCsv,
    Json,
    PartitionedJson,
    Iceberg,
    PartitionedIceberg,
    Deltalake,
    PartitionedDeltalake,
    Lance,
    DataSink(String),
}

pub(crate) struct WriteState {
    writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
}

impl WriteState {
    pub fn new(
        writer: Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    ) -> Self {
        Self { writer }
    }
}

pub(crate) struct WriteSink {
    write_format: WriteFormat,
    writer_factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>>,
    partition_by: Option<Vec<BoundExpr>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub(crate) fn new(
        write_format: WriteFormat,
        writer_factory: Arc<
            dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>,
        >,
        partition_by: Option<Vec<BoundExpr>>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            write_format,
            writer_factory,
            partition_by,
            file_schema,
        }
    }
}

impl BlockingSink for WriteSink {
    type State = WriteState;

    #[instrument(skip_all, name = "WriteSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let builder = spawner.runtime_stats.clone();

        spawner
            .spawn(
                async move {
                    let bytes_written = state.writer.write(input).await?;

                    builder
                        .as_any_arc()
                        .downcast_ref::<WriteStats>()
                        .expect("WriteStats should be the additional stats builder")
                        .bytes_written
                        .fetch_add(bytes_written as u64, std::sync::atomic::Ordering::Relaxed);

                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WriteSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let file_schema = self.file_schema.clone();
        spawner
            .spawn(
                async move {
                    let mut results = vec![];
                    for mut state in states {
                        results.extend(state.writer.close().await?);
                    }
                    let mp = Arc::new(MicroPartition::new_loaded(
                        file_schema,
                        results.into(),
                        None,
                    ));
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![mp]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match &self.write_format {
            WriteFormat::Parquet => "Parquet Write".into(),
            WriteFormat::PartitionedParquet => "PartitionedParquet Write".into(),
            WriteFormat::Csv => "Csv Write".into(),
            WriteFormat::PartitionedCsv => "PartitionedCsv Write".into(),
            WriteFormat::Json => "Json Write".into(),
            WriteFormat::PartitionedJson => "PartitionedJson Write".into(),
            WriteFormat::Iceberg => "Iceberg Write".into(),
            WriteFormat::PartitionedIceberg => "PartitionedIceberg Write".into(),
            WriteFormat::Deltalake => "Deltalake Write".into(),
            WriteFormat::PartitionedDeltalake => "PartitionedDeltalake Write".into(),
            WriteFormat::Lance => "Lance Write".into(),
            WriteFormat::DataSink(name) => name.clone().into(),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::Write
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        let writer = self.writer_factory.create_writer(0, None)?;
        Ok(WriteState::new(writer))
    }

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(WriteStats::default())
    }

    fn dispatch_spawner(
        &self,
        _morsel_size_requirement: Option<MorselSizeRequirement>,
    ) -> Arc<dyn DispatchSpawner> {
        if let Some(partition_by) = &self.partition_by {
            Arc::new(PartitionedDispatcher::new(partition_by.clone()))
        } else {
            // Unnecessary to buffer by morsel size because we are writing.
            // Writers also have their own internal buffering.
            Arc::new(UnorderedDispatcher::unbounded())
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        lines.push(format!("Write: {:?}", self.write_format));
        if let Some(partition_by) = &self.partition_by {
            lines.push(format!("Partition by: {:?}", partition_by));
        }
        lines
    }

    fn max_concurrency(&self) -> usize {
        if self.partition_by.is_some() {
            get_compute_pool_num_threads()
        } else {
            1
        }
    }
}
