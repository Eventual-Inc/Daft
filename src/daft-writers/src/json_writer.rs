use std::{path::PathBuf, sync::Arc};

use arrow_json::{LineDelimitedWriter, WriterBuilder, writer::LineDelimited};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::{IOConfig, SourceType, parse_url, utils::ObjectPath};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    AsyncFileWriter,
    batch_file_writer::BatchFileWriter,
    storage_backend::{FileStorageBackend, ObjectStorageBackend, StorageBackend},
    utils::build_filename,
};

type JsonFinishFn<B> =
    Arc<dyn Fn(LineDelimitedWriter<<B as StorageBackend>::Writer>) -> DaftResult<()> + Send + Sync>;

/// Helper function that checks if we support native writes given the file format, root directory, and schema.
pub(crate) fn native_json_writer_supported(file_schema: &SchemaRef) -> DaftResult<bool> {
    // TODO(desmond): Currently we do not support extension and timestamp types.
    #[allow(deprecated, reason = "arrow2 migration")]
    let datatypes_convertable = file_schema.to_arrow2()?.fields.iter().all(|field| {
        field.data_type().can_convert_to_arrow_rs() && field.data_type().can_convert_to_json()
    });
    Ok(datatypes_convertable)
}

pub(crate) fn create_native_json_writer(
    root_dir: &str,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    // Parse the root directory and add partition values if present.
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = build_filename(
        source_type,
        root_dir.as_ref(),
        partition_values,
        file_idx,
        "json",
    )?;
    match source_type {
        SourceType::File => {
            let storage_backend = FileStorageBackend {};
            Ok(Box::new(make_json_writer(
                filename,
                partition_values.cloned(),
                storage_backend,
            )))
        }
        SourceType::S3 => {
            let ObjectPath { scheme, .. } = daft_io::utils::parse_object_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError("IO config is required for S3 writes".to_string())
            })?;
            let storage_backend = ObjectStorageBackend::new(scheme, io_config);
            Ok(Box::new(make_json_writer(
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

fn make_json_writer<B: StorageBackend + Send + Sync>(
    filename: PathBuf,
    partition_values: Option<RecordBatch>,
    storage_backend: B,
) -> BatchFileWriter<B, LineDelimitedWriter<B::Writer>> {
    let builder = Arc::new(|backend: B::Writer| {
        WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(backend)
    });
    let write_fn = Arc::new(
        |writer: &mut LineDelimitedWriter<B::Writer>, batches: &[arrow_array::RecordBatch]| {
            let refs: Vec<&arrow_array::RecordBatch> = batches.iter().collect();
            writer.write_batches(&refs)?;
            Ok(())
        },
    );
    let finish_fn: Option<JsonFinishFn<B>> =
        Some(Arc::new(|mut writer: LineDelimitedWriter<B::Writer>| {
            writer.finish()?;
            Ok(())
        }));
    BatchFileWriter::new(
        filename,
        partition_values,
        storage_backend,
        0.5,
        builder,
        write_fn,
        finish_fn,
    )
}
