use std::{path::PathBuf, sync::Arc};

use arrow_csv::WriterBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::{IOConfig, SourceType, parse_url};
use daft_recordbatch::RecordBatch;
use daft_micropartition::MicroPartition;

use crate::{
    storage_backend::{FileStorageBackend, S3StorageBackend, StorageBackend},
    utils::build_filename,
    batch_file_writer::BatchFileWriter,
    AsyncFileWriter,
};

pub(crate) fn native_csv_writer_supported(file_schema: &SchemaRef) -> DaftResult<bool> {
    let datatypes_convertable = file_schema.to_arrow()?.fields.iter().all(|field| {
        field.data_type().can_convert_to_arrow_rs() && field.data_type().can_convert_to_csv()
    });
    Ok(datatypes_convertable)
}

pub(crate) fn create_native_csv_writer(
    root_dir: &str,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = build_filename(
        source_type,
        root_dir.as_ref(),
        partition_values,
        file_idx,
        "csv",
    )?;
    match source_type {
        SourceType::File => {
            let storage_backend = FileStorageBackend {};
            Ok(Box::new(make_csv_writer(
                filename,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        SourceType::S3 => {
            let (scheme, _, _) = daft_io::s3_like::parse_s3_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError("IO config is required for S3 writes".to_string())
            })?;
            let storage_backend = S3StorageBackend::new(scheme, io_config);
            Ok(Box::new(make_csv_writer(
                filename,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        _ => Err(DaftError::ValueError(format!(
            "Unsupported source type: {:?}",
            source_type
        ))),
    }
}

fn make_csv_writer<B: StorageBackend + Send + Sync>(
    filename: PathBuf,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
) -> BatchFileWriter<B, arrow_csv::Writer<B::Writer>> {
    let builder = Arc::new(|backend: B::Writer| {
        WriterBuilder::new().with_header(true).build(backend)
    });
    let write_fn = Arc::new(|writer: &mut arrow_csv::Writer<B::Writer>, batches: &[arrow_array::RecordBatch]| {
        for rb in batches {
            writer.write(rb)?;
        }
        Ok(())
    });
    BatchFileWriter::new(
        filename,
        partition_values,
        storage_backend,
        1.0,
        builder,
        write_fn,
        None,
    )
}
