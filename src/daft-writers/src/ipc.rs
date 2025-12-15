use std::{fs::File, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, WriteResult, WriterFactory};

pub struct IPCWriter {
    is_closed: bool,
    bytes_written: usize,
    file_path: String,
    compression: Option<daft_arrow::io::ipc::write::Compression>,
    writer: Option<daft_arrow::io::ipc::write::StreamWriter<File>>,
}

impl IPCWriter {
    pub fn new(
        file_path: &str,
        compression: Option<daft_arrow::io::ipc::write::Compression>,
    ) -> Self {
        Self {
            is_closed: false,
            bytes_written: 0,
            file_path: file_path.to_string(),
            compression,
            writer: None,
        }
    }

    fn get_or_create_writer(
        &mut self,
        schema: &Schema,
    ) -> DaftResult<&mut daft_arrow::io::ipc::write::StreamWriter<File>> {
        if self.writer.is_none() {
            let file = File::create(self.file_path.as_str())?;
            let options = daft_arrow::io::ipc::write::WriteOptions {
                compression: self.compression,
            };
            let mut writer = daft_arrow::io::ipc::write::StreamWriter::new(file, options);
            #[allow(deprecated, reason = "arrow2 migration")]
            writer.start(&schema.to_arrow2()?, None)?;
            self.writer = Some(writer);
        }
        Ok(self.writer.as_mut().unwrap())
    }
}

#[async_trait]
impl AsyncFileWriter for IPCWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        assert!(!self.is_closed, "Writer is closed");

        let size_bytes = data.size_bytes();
        let rows_written = data.len();
        let writer = self.get_or_create_writer(&data.schema())?;
        for table in data.record_batches() {
            #[allow(deprecated, reason = "arrow2 migration")]
            let chunk = table.to_chunk();
            writer.write(&chunk, None)?;
        }
        self.bytes_written += writer.bytes_written();
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(mut writer) = self.writer.take() {
            writer.finish()?;
        }
        // return the path
        let path_col = Series::from_arrow(
            Arc::new(Field::new(RETURN_PATHS_COLUMN_NAME, DataType::Utf8)),
            Box::new(daft_arrow::array::Utf8Array::<i64>::from_iter_values(
                std::iter::once(self.file_path.clone()),
            )),
        )?;
        let res = RecordBatch::from_nonempty_columns(vec![path_col])?;
        Ok(Some(res))
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }
}

pub struct IPCWriterFactory {
    dir: String,
    compression: Option<daft_arrow::io::ipc::write::Compression>,
}

impl IPCWriterFactory {
    pub fn new(dir: String, compression: Option<daft_arrow::io::ipc::write::Compression>) -> Self {
        Self { dir, compression }
    }
}

impl WriterFactory for IPCWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let file_path = format!("{}/{}.arrow", self.dir, file_idx);
        let writer = IPCWriter::new(&file_path, self.compression);
        Ok(Box::new(writer))
    }
}
