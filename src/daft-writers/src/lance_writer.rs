use std::sync::Arc;

use arrow_array::{RecordBatch as ArrowRecordBatch, RecordBatchIterator};
use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::OutputFileInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use lance::{
    dataset::{InsertBuilder, WriteMode, WriteParams, transaction::Operation},
    table::format::Fragment,
};

use crate::{AsyncFileWriter, WriterFactory};

pub struct LanceNativeWriter {
    path: String,
    mode: WriteMode,
    schema: SchemaRef,
    bytes_written: usize,
    results: Vec<RecordBatch>,
}

impl LanceNativeWriter {
    pub fn new(path: String, mode: WriteMode, schema: SchemaRef) -> Self {
        Self {
            path,
            mode,
            schema,
            bytes_written: 0,
            results: vec![],
        }
    }

    fn write_params(&self) -> WriteParams {
        WriteParams {
            mode: self.mode,
            ..Default::default()
        }
    }

    fn fragments_to_record_batch(&self, _fragments: Vec<Fragment>) -> RecordBatch {
        // TODO add impl by zhenchao
        RecordBatch::empty(None)
    }
}

#[async_trait]
impl AsyncFileWriter for LanceNativeWriter {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<usize> {
        let record_batches = data.get_tables()?;
        let arrow_record_batches: Vec<ArrowRecordBatch> = record_batches
            .iter()
            .map(|rb| rb.clone().try_into())
            .collect::<DaftResult<_>>()?;

        let schema = Arc::new(self.schema.to_arrow()?.into());
        let batches = RecordBatchIterator::new(arrow_record_batches.into_iter().map(Ok), schema);

        let txn = InsertBuilder::new(self.path.as_str())
            .with_params(&self.write_params())
            .execute_uncommitted_stream(batches)
            .await
            .unwrap();

        let fragments = match txn.operation {
            Operation::Append { fragments } => fragments,
            Operation::Overwrite { fragments, .. } => fragments,
            _ => {
                return Err(DaftError::InternalError(format!(
                    "unexpected lance operation {}",
                    txn.operation.name()
                )));
            }
        };

        let result = self.fragments_to_record_batch(fragments);
        self.results.push(result);

        self.bytes_written += data
            .size_bytes()
            .expect("MicroPartition should have size_bytes for LanceNativeWriter");

        Ok(self.bytes_written)
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        Ok(std::mem::take(&mut self.results))
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }
}

// FIXME by zhenchao use PhysicalWriterFactory?
pub struct LanceNativeWriterFactory {
    output_file_info: OutputFileInfo<BoundExpr>,
    schema: SchemaRef,
}

impl LanceNativeWriterFactory {
    pub fn new(
        output_file_info: OutputFileInfo<BoundExpr>,
        file_schema: SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            output_file_info,
            schema: file_schema,
        })
    }
}

impl WriterFactory for LanceNativeWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        _file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let writer = LanceNativeWriter::new(
            self.output_file_info.root_dir.clone(),
            WriteMode::Create, // FIXME by zhenchao,
            self.schema.clone(),
        );
        Ok(Box::new(writer))
    }
}
