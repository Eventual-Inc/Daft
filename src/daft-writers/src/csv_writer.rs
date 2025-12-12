use std::{path::PathBuf, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray,
    RecordBatch as ArrowRecordBatch, RecordBatchWriter,
    builder::{Int64Builder, LargeStringBuilder},
};
use arrow_csv::WriterBuilder;
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};

macro_rules! cast_duration_to_int64 {
    ($arr:expr, $pt:ty) => {{
        let a = $arr
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<$pt>>()
            .unwrap();
        let mut builder = Int64Builder::with_capacity(a.len());
        for idx in 0..a.len() {
            if a.is_null(idx) {
                builder.append_null();
            } else {
                builder.append_value(a.value(idx));
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

macro_rules! try_encode_binary_utf8 {
    ($arr:expr, $ty:ty) => {{
        let a = $arr.as_any().downcast_ref::<$ty>().unwrap();
        let mut builder = LargeStringBuilder::with_capacity(a.len(), 0);
        for idx in 0..a.len() {
            if a.is_null(idx) {
                builder.append_null();
            } else {
                let bytes = a.value(idx);
                if let Ok(s) = std::str::from_utf8(bytes) {
                    builder.append_value(s);
                } else {
                    return Err(ArrowError::ComputeError("Invalid UTF8 payload".to_string()));
                }
            }
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

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

pub(crate) fn native_csv_writer_supported(file_schema: &SchemaRef) -> DaftResult<bool> {
    #[allow(deprecated, reason = "arrow2 migration")]
    let datatypes_convertable = file_schema.to_arrow2()?.fields.iter().all(|field| {
        field.data_type().can_convert_to_arrow_rs() && !is_nested_type(field.data_type())
    });
    Ok(datatypes_convertable)
}

/// Returns true if this type is nested (List, FixedSizeList, LargeList, Struct, Union, or Map), or a dictionary of a nested type
pub(crate) fn is_nested_type(dt: &daft_arrow::datatypes::DataType) -> bool {
    match dt {
        daft_arrow::datatypes::DataType::Dictionary(_, v, ..) => is_nested_type(v),
        daft_arrow::datatypes::DataType::List(_)
        | daft_arrow::datatypes::DataType::FixedSizeList(_, _)
        | daft_arrow::datatypes::DataType::LargeList(_)
        | daft_arrow::datatypes::DataType::Struct(_)
        | daft_arrow::datatypes::DataType::Union(_, _, _)
        | daft_arrow::datatypes::DataType::Map(_, _) => true,
        _ => false,
    }
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
            let ObjectPath { scheme, .. } = daft_io::utils::parse_object_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError("IO config is required for S3 writes".to_string())
            })?;
            let storage_backend = ObjectStorageBackend::new(scheme, io_config);
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
    let builder =
        Arc::new(|backend: B::Writer| WriterBuilder::new().with_header(true).build(backend));
    let write_fn = Arc::new(
        |writer: &mut arrow_csv::Writer<B::Writer>, batches: &[arrow_array::RecordBatch]| {
            fn transform_batch(batch: ArrowRecordBatch) -> Result<ArrowRecordBatch, ArrowError> {
                let schema = batch.schema();
                let mut new_fields: Vec<ArrowField> = Vec::with_capacity(schema.fields().len());
                let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
                for (i, field_arc) in schema.fields().iter().enumerate() {
                    let field = field_arc.as_ref();
                    match field.data_type() {
                        ArrowDataType::Duration(_) => {
                            let arr = batch.column(i);
                            let col = match arr.data_type() {
                                ArrowDataType::Duration(TimeUnit::Second) => {
                                    cast_duration_to_int64!(
                                        arr,
                                        arrow_array::types::DurationSecondType
                                    )
                                }
                                ArrowDataType::Duration(TimeUnit::Millisecond) => {
                                    cast_duration_to_int64!(
                                        arr,
                                        arrow_array::types::DurationMillisecondType
                                    )
                                }
                                ArrowDataType::Duration(TimeUnit::Microsecond) => {
                                    cast_duration_to_int64!(
                                        arr,
                                        arrow_array::types::DurationMicrosecondType
                                    )
                                }
                                ArrowDataType::Duration(TimeUnit::Nanosecond) => {
                                    cast_duration_to_int64!(
                                        arr,
                                        arrow_array::types::DurationNanosecondType
                                    )
                                }
                                _ => unreachable!(),
                            };
                            new_cols.push(col);
                            new_fields.push(ArrowField::new(
                                field.name(),
                                ArrowDataType::Int64,
                                field.is_nullable(),
                            ));
                        }
                        ArrowDataType::Binary
                        | ArrowDataType::LargeBinary
                        | ArrowDataType::FixedSizeBinary(_) => {
                            // Encode binary to UTF-8.
                            let arr = batch.column(i);
                            let col = match arr.data_type() {
                                ArrowDataType::Binary => try_encode_binary_utf8!(arr, BinaryArray),
                                ArrowDataType::LargeBinary => {
                                    try_encode_binary_utf8!(arr, LargeBinaryArray)
                                }
                                ArrowDataType::FixedSizeBinary(_) => {
                                    try_encode_binary_utf8!(arr, FixedSizeBinaryArray)
                                }
                                _ => unreachable!(),
                            };
                            new_cols.push(col);
                            new_fields.push(ArrowField::new(
                                field.name(),
                                ArrowDataType::LargeUtf8,
                                field.is_nullable(),
                            ));
                        }
                        _ => {
                            new_fields.push(field.clone());
                            new_cols.push(batch.column(i).clone());
                        }
                    }
                }
                let new_schema = Arc::new(ArrowSchema::new(new_fields));
                ArrowRecordBatch::try_new(new_schema, new_cols)
            }

            for rb in batches {
                let rb2 = transform_batch(rb.clone())
                    .map_err(|e| DaftError::ComputeError(e.to_string()))?;
                writer.write(&rb2)?;
            }
            Ok(())
        },
    );
    let close_fn = Arc::new(|writer: arrow_csv::Writer<B::Writer>| {
        writer.close()?;
        Ok(())
    });
    BatchFileWriter::new(
        filename,
        partition_values,
        storage_backend,
        1.0,
        builder,
        write_fn,
        Some(close_fn),
    )
}
