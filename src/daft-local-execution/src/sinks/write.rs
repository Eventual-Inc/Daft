use std::sync::{Arc, atomic::Ordering};

use common_error::DaftResult;
use common_metrics::{
    BYTES_WRITTEN_KEY, Counter, Meter, ROWS_WRITTEN_KEY, StatSnapshot, UNIT_BYTES, UNIT_ROWS,
    ops::{NodeInfo, NodeType},
    snapshot::WriteSnapshot,
};
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_writers::{AsyncFileWriter, WriteResult, WriterFactory};
use opentelemetry::KeyValue;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    runtime_stats::RuntimeStats,
};

pub(crate) struct WriteStats {
    duration_us: Counter,
    rows_in: Counter,
    bytes_in: Counter,
    rows_written: Counter,
    bytes_written: Counter,
    num_tasks: Counter,

    node_kv: Vec<KeyValue>,
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
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();

        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            bytes_in: meter.bytes_in_metric(),
            rows_written: meter.u64_counter_with_desc_and_unit(
                ROWS_WRITTEN_KEY,
                None,
                Some(UNIT_ROWS.into()),
            ),
            bytes_written: meter.u64_counter_with_desc_and_unit(
                BYTES_WRITTEN_KEY,
                None,
                Some(UNIT_BYTES.into()),
            ),
            num_tasks: meter.num_tasks_metric(),

            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.duration_us.load(ordering);
        let rows_in = self.rows_in.load(ordering);
        let rows_written = self.rows_written.load(ordering);
        let bytes_written = self.bytes_written.load(ordering);
        StatSnapshot::Write(WriteSnapshot {
            cpu_us,
            rows_in,
            rows_written,
            bytes_written,
            bytes_in: self.bytes_in.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    // The 'rows_out' for a WriteSink is the number of files written, which we only know upon 'finalize',
    // so there's no benefit to adding it in runtime stats as it is not real time.
    fn add_rows_out(&self, _rows: u64) {}

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.add(bytes, self.node_kv.as_slice());
    }

    // bytes_out for WriteSink doesn't make sense — bytes_written is the meaningful metric.
    fn add_bytes_out(&self, _bytes: u64) {}

    fn add_num_tasks(&self, num_tasks: u64) {
        self.num_tasks.add(num_tasks, self.node_kv.as_slice());
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
    writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
    runtime_stats: Option<Arc<WriteStats>>,
    total_rows_input: usize,
    reported_rows: usize,
    reported_bytes: usize,
}

impl WriteState {
    pub fn new(
        writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
    ) -> Self {
        Self {
            writer,
            runtime_stats: None,
            total_rows_input: 0,
            reported_rows: 0,
            reported_bytes: 0,
        }
    }
}

pub(crate) struct WriteSink {
    write_format: WriteFormat,
    writer_factory: Arc<dyn WriterFactory<Input = MicroPartition, Result = Vec<RecordBatch>>>,
    partition_by: Option<Vec<BoundExpr>>,
    file_schema: SchemaRef,
}

impl WriteSink {
    pub(crate) fn new(
        write_format: WriteFormat,
        writer_factory: Arc<dyn WriterFactory<Input = MicroPartition, Result = Vec<RecordBatch>>>,
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
    type Stats = WriteStats;

    #[instrument(skip_all, name = "WriteSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        spawner
            .spawn(
                async move {
                    if state.runtime_stats.is_none() {
                        state.runtime_stats = Some(runtime_stats.clone());
                    }
                    state.total_rows_input += input.len();
                    let write_result = state.writer.write(input).await?;
                    state.reported_rows += write_result.rows_written;
                    state.reported_bytes += write_result.bytes_written;
                    runtime_stats.add_write_result(write_result);
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
    ) -> BlockingSinkFinalizeResult {
        let file_schema = self.file_schema.clone();
        spawner
            .spawn(
                async move {
                    let mut results = vec![];
                    for mut state in states {
                        results.extend(state.writer.close().await?);
                        if let Some(stats) = &state.runtime_stats {
                            let total_bytes: usize = state.writer.bytes_per_file().iter().sum();
                            let bytes_delta = total_bytes.saturating_sub(state.reported_bytes);
                            let rows_delta =
                                state.total_rows_input.saturating_sub(state.reported_rows);
                            if bytes_delta > 0 || rows_delta > 0 {
                                stats.add_write_result(WriteResult {
                                    bytes_written: bytes_delta,
                                    rows_written: rows_delta,
                                });
                            }
                        }
                    }
                    let mp = MicroPartition::new_loaded(file_schema, results.into(), None);
                    Ok(BlockingSinkOutput::Partitions(vec![mp]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match &self.write_format {
            WriteFormat::Parquet => "Parquet Write".into(),
            WriteFormat::PartitionedParquet => "Partitioned Parquet Write".into(),
            WriteFormat::Csv => "CSV Write".into(),
            WriteFormat::PartitionedCsv => "Partitioned CSV Write".into(),
            WriteFormat::Json => "JSON Write".into(),
            WriteFormat::PartitionedJson => "Partitioned JSON Write".into(),
            WriteFormat::Iceberg => "Iceberg Write".into(),
            WriteFormat::PartitionedIceberg => "Partitioned Iceberg Write".into(),
            WriteFormat::Deltalake => "DeltaLake Write".into(),
            WriteFormat::PartitionedDeltalake => "Partitioned DeltaLake Write".into(),
            WriteFormat::Lance => "Lance Write".into(),
            WriteFormat::DataSink(name) => name.clone().into(),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::Write
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        let writer = self.writer_factory.create_writer(0, None)?;
        Ok(WriteState::new(writer))
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
