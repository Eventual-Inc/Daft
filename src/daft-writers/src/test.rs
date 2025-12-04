use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema, UInt8Array, UInt64Array, Utf8Array},
    series::IntoSeries,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, TargetFileSizeWriterFactory,
    TargetInMemorySizeBytesCalculator, WriteResult, WriterFactory,
};

pub struct DummyWriterFactory;

impl WriterFactory for DummyWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(DummyWriter {
            file_idx: file_idx.to_string(),
            partition_values: partition_values.cloned(),
            write_count: 0,
            byte_count: 0,
        })
            as Box<
                dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

pub struct DummyWriter {
    pub file_idx: String,
    pub partition_values: Option<RecordBatch>,
    pub write_count: usize,
    pub byte_count: usize,
}

#[async_trait]
impl AsyncFileWriter for DummyWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, input: Self::Input) -> DaftResult<WriteResult> {
        self.write_count += 1;
        let size_bytes = input.size_bytes();
        self.byte_count += size_bytes;
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written: input.len(),
        })
    }

    fn bytes_written(&self) -> usize {
        self.byte_count
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.byte_count]
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        let path_series = Utf8Array::from_values(
            RETURN_PATHS_COLUMN_NAME,
            std::iter::once(self.file_idx.clone()),
        )
        .into_series();
        let write_count_series =
            UInt64Array::from_values("write_count", std::iter::once(self.write_count as u64))
                .into_series();
        let path_table = RecordBatch::new_unchecked(
            Schema::new(vec![
                path_series.field().clone(),
                write_count_series.field().clone(),
            ]),
            vec![path_series.into(), write_count_series.into()],
            1,
        );
        if let Some(partition_values) = self.partition_values.take() {
            let unioned = path_table.union(&partition_values)?;
            Ok(Some(unioned))
        } else {
            Ok(Some(path_table))
        }
    }
}

#[allow(dead_code)]
pub struct FailingWriterFactory {
    pub fail_on_write: bool,
    pub fail_on_close: bool,
}

impl FailingWriterFactory {
    #[allow(dead_code)]
    pub fn new_fail_on_write() -> Self {
        Self {
            fail_on_write: true,
            fail_on_close: false,
        }
    }

    #[allow(dead_code)]
    pub fn new_fail_on_close() -> Self {
        Self {
            fail_on_write: false,
            fail_on_close: true,
        }
    }
}

impl WriterFactory for FailingWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(FailingWriter {
            file_idx: file_idx.to_string(),
            partition_values: partition_values.cloned(),
            write_count: 0,
            byte_count: 0,
            fail_on_write: self.fail_on_write,
            fail_on_close: self.fail_on_close,
        })
            as Box<
                dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

pub struct FailingWriter {
    pub file_idx: String,
    pub partition_values: Option<RecordBatch>,
    pub write_count: usize,
    pub byte_count: usize,
    pub fail_on_write: bool,
    pub fail_on_close: bool,
}

#[async_trait]
impl AsyncFileWriter for FailingWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, input: Self::Input) -> DaftResult<WriteResult> {
        if self.fail_on_write {
            return Err(common_error::DaftError::ValueError(
                "Intentional failure in FailingWriter::write".to_string(),
            ));
        }

        self.write_count += 1;
        let size_bytes = input.size_bytes();
        self.byte_count += size_bytes;
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written: input.len(),
        })
    }

    fn bytes_written(&self) -> usize {
        self.byte_count
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.byte_count]
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if self.fail_on_close {
            return Err(common_error::DaftError::ValueError(
                "Intentional failure in FailingWriter::close".to_string(),
            ));
        }

        // Same behavior as DummyWriter when not failing
        let path_series = Utf8Array::from_values(
            RETURN_PATHS_COLUMN_NAME,
            std::iter::once(self.file_idx.clone()),
        )
        .into_series();
        let write_count_series =
            UInt64Array::from_values("write_count", std::iter::once(self.write_count as u64))
                .into_series();
        let path_table = RecordBatch::new_unchecked(
            Schema::new(vec![
                path_series.field().clone(),
                write_count_series.field().clone(),
            ]),
            vec![path_series.into(), write_count_series.into()],
            1,
        );
        if let Some(partition_values) = self.partition_values.take() {
            let unioned = path_table.union(&partition_values)?;
            Ok(Some(unioned))
        } else {
            Ok(Some(path_table))
        }
    }
}

#[allow(dead_code)]
pub fn make_dummy_target_file_size_writer_factory(
    target_size_bytes: usize,
    initial_inflation_factor: f64,
    factory: Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>,
) -> Arc<dyn WriterFactory<Input = Arc<MicroPartition>, Result = Vec<RecordBatch>>> {
    let target_file_size_calculator =
        TargetInMemorySizeBytesCalculator::new(target_size_bytes, initial_inflation_factor);
    Arc::new(TargetFileSizeWriterFactory::new(
        factory,
        Arc::new(target_file_size_calculator),
    ))
}

pub fn make_dummy_mp(size_bytes: usize) -> Arc<MicroPartition> {
    let range = (0..size_bytes).map(|i| Some(i as u8));
    let series = UInt8Array::from_regular_iter(Field::new("ints", DataType::UInt8), range)
        .unwrap()
        .into_series();
    let schema = Arc::new(Schema::new(vec![series.field().clone()]));
    let table = RecordBatch::new_unchecked(schema.clone(), vec![series.into()], size_bytes);
    Arc::new(MicroPartition::new_loaded(
        schema.into(),
        vec![table].into(),
        None,
    ))
}
