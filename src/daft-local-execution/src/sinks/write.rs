use std::sync::{Arc, atomic::Ordering};

use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, Counter, ROWS_IN_KEY, StatSnapshot, ops::NodeType, snapshot::WriteSnapshot,
};
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::{AsyncFileWriter, WriteResult, WriterFactory};
use opentelemetry::{KeyValue, global};
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, runtime_stats::RuntimeStats};

struct WriteStats {
    cpu_us: Counter,
    rows_in: Counter,
    rows_written: Counter,
    bytes_written: Counter,

    node_kv: Vec<KeyValue>,
}

impl WriteStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, CPU_US_KEY, None),
            rows_in: Counter::new(&meter, ROWS_IN_KEY, None),
            rows_written: Counter::new(&meter, "rows written", None),
            bytes_written: Counter::new(&meter, "bytes written", None),

            node_kv,
        }
    }
}

impl WriteStats {
    fn add_write_result(&self, write_result: WriteResult) {
        self.rows_written
            .add(write_result.rows_written as u64, self.node_kv.as_slice());
        self.bytes_written
            .add(write_result.bytes_written as u64, self.node_kv.as_slice());
    }
}

impl RuntimeStats for WriteStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_written = self.rows_written.load(ordering);
        let bytes_written = self.bytes_written.load(ordering);
        StatSnapshot::Write(WriteSnapshot {
            cpu_us,
            rows_in,
            rows_written,
            bytes_written,
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    // The 'rows_out' for a WriteSink is the number of files written, which we only know upon 'finalize',
    // so there's no benefit to adding it in runtime stats as it is not real time.
    fn add_rows_out(&self, _rows: u64) {}

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
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
                    let write_result = state.writer.write(input).await?;

                    builder
                        .as_any_arc()
                        .downcast_ref::<WriteStats>()
                        .expect("WriteStats should be the additional stats builder")
                        .add_write_result(write_result);

                    Ok(state)
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

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(WriteStats::new(id))
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
        1
    }
}
