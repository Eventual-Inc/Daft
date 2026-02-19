use std::{fmt::Write, path::PathBuf, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray,
    RecordBatch as ArrowRecordBatch, RecordBatchWriter,
    builder::{Int64Builder, LargeStringBuilder},
    cast::AsArray,
    types::Date32Type,
};
use arrow_csv::WriterBuilder;
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use chrono::{DateTime, NaiveDate, format::StrftimeItems};
use chrono_tz::Tz;

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

/// Safely format a NaiveDate using strftime
fn safe_date_format(date: &NaiveDate, format: &str) -> Result<String, ArrowError> {
    let items = StrftimeItems::new(format);
    let delayed = date.format_with_items(items);
    let mut result = String::new();
    write!(result, "{}", delayed)
        .map_err(|_| ArrowError::ComputeError(format!("Invalid date format string: {}", format)))?;
    Ok(result)
}

/// Safely format a DateTime using strftime
fn safe_datetime_format<Tz: chrono::TimeZone>(
    dt: &DateTime<Tz>,
    format: &str,
) -> Result<String, ArrowError>
where
    Tz::Offset: std::fmt::Display,
{
    let items = StrftimeItems::new(format);
    let delayed = dt.format_with_items(items);
    let mut result = String::new();
    write!(result, "{}", delayed).map_err(|_| {
        ArrowError::ComputeError(format!("Invalid timestamp format string: {}", format))
    })?;
    Ok(result)
}

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_io::{IOConfig, SourceType, parse_url, utils::ObjectPath};
use daft_logical_plan::sink_info::CsvFormatOption;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{
    AsyncFileWriter,
    batch_file_writer::BatchFileWriter,
    storage_backend::{FileStorageBackend, ObjectStorageBackend, StorageBackend},
    utils::build_filename,
};

/// Returns true if this type is nested (List, FixedSizeList, LargeList, Struct, Union, or Map), or a dictionary of a nested type
fn native_csv_field_supported(field: &Arc<arrow_schema::Field>) -> bool {
    if field.extension_type_name().is_some() {
        return false;
    }

    !matches!(
        field.data_type(),
        arrow_schema::DataType::List(_)
            | arrow_schema::DataType::FixedSizeList(_, _)
            | arrow_schema::DataType::LargeList(_)
            | arrow_schema::DataType::Struct(_)
            | arrow_schema::DataType::Union(_, _)
            | arrow_schema::DataType::Map(_, _)
    )
}

pub(crate) fn native_csv_writer_supported(file_schema: &SchemaRef) -> DaftResult<bool> {
    Ok(file_schema
        .to_arrow()?
        .fields
        .iter()
        .all(native_csv_field_supported))
}

pub(crate) fn create_native_csv_writer(
    root_dir: &str,
    file_idx: usize,
    partition_values: Option<&RecordBatch>,
    io_config: Option<IOConfig>,
    csv_option: CsvFormatOption,
) -> DaftResult<Box<dyn AsyncFileWriter<Input = Arc<MicroPartition>, Result = Option<RecordBatch>>>>
{
    let (source_type, root_dir) = parse_url(root_dir)?;
    let filename = build_filename(
        &source_type,
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
                csv_option,
            )))
        }
        source if source.supports_native_writer() => {
            let ObjectPath { scheme, .. } = daft_io::utils::parse_object_url(root_dir.as_ref())?;
            let io_config = io_config.ok_or_else(|| {
                DaftError::InternalError(
                    "IO config is required for object store writes".to_string(),
                )
            })?;
            let storage_backend = ObjectStorageBackend::new(scheme, io_config);
            Ok(Box::new(make_csv_writer(
                filename,
                partition_values.cloned(),
                storage_backend,
                csv_option,
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
    csv_option: CsvFormatOption,
) -> BatchFileWriter<B, arrow_csv::Writer<B::Writer>> {
    let hdr = csv_option.header.unwrap_or(true);
    let quote = csv_option.quote;
    let escape = csv_option.escape;
    let delimiter = csv_option.delimiter;
    let date_format = csv_option.date_format.clone();
    let timestamp_format = csv_option.timestamp_format;
    let builder = Arc::new(move |backend: B::Writer| {
        let mut builder = WriterBuilder::new().with_header(hdr);

        if let Some(quote) = quote {
            builder = builder.with_quote(quote);
        }

        if let Some(escape) = escape {
            builder = builder.with_escape(escape);
        }

        if let Some(delimiter) = delimiter {
            builder = builder.with_delimiter(delimiter);
        }
        builder.build(backend)
    });

    let write_fn = Arc::new(
        move |writer: &mut arrow_csv::Writer<B::Writer>, batches: &[arrow_array::RecordBatch]| {
            let date_fmt = date_format.clone();
            let timestamp_fmt = timestamp_format.clone();

            fn transform_batch(
                batch: ArrowRecordBatch,
                date_fmt: Option<&String>,
                timestamp_fmt: Option<&String>,
            ) -> Result<ArrowRecordBatch, ArrowError> {
                let schema = batch.schema();
                let mut new_fields: Vec<ArrowField> = Vec::with_capacity(schema.fields().len());
                let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
                for (i, field_arc) in schema.fields().iter().enumerate() {
                    let field = field_arc.as_ref();
                    match field.data_type() {
                        ArrowDataType::Date32 => {
                            if let Some(fmt) = date_fmt {
                                let arr = batch.column(i).as_primitive::<Date32Type>();
                                let mut builder = LargeStringBuilder::with_capacity(arr.len(), 0);
                                for idx in 0..arr.len() {
                                    if arr.is_null(idx) {
                                        builder.append_null();
                                    } else {
                                        let days = arr.value(idx);
                                        let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                                            .unwrap()
                                            .checked_add_signed(chrono::Duration::days(days as i64))
                                            .ok_or_else(|| {
                                                ArrowError::ComputeError("Invalid date".to_string())
                                            })?;
                                        builder.append_value(safe_date_format(&date, fmt)?);
                                    }
                                }
                                new_cols.push(Arc::new(builder.finish()) as ArrayRef);
                                new_fields.push(ArrowField::new(
                                    field.name(),
                                    ArrowDataType::LargeUtf8,
                                    field.is_nullable(),
                                ));
                            } else {
                                new_fields.push(field.clone());
                                new_cols.push(batch.column(i).clone());
                            }
                        }
                        // Note: Date64 is not handled here because Daft converts Date64 to
                        // Timestamp[ms] upon ingestion. Any Date64 data will arrive as Timestamp.
                        ArrowDataType::Timestamp(_time_unit, tz_opt) => {
                            if let Some(fmt) = timestamp_fmt {
                                let arr = batch.column(i);
                                let mut builder = LargeStringBuilder::with_capacity(arr.len(), 0);

                                // Parse timezone if present - return error if parsing fails
                                let parsed_tz: Option<Tz> = if let Some(tz_str) = tz_opt.as_ref() {
                                    Some(tz_str.parse::<Tz>().map_err(|_| {
                                        ArrowError::ComputeError(format!(
                                            "Invalid timezone: {}",
                                            tz_str
                                        ))
                                    })?)
                                } else {
                                    None
                                };

                                for idx in 0..arr.len() {
                                    if arr.is_null(idx) {
                                        builder.append_null();
                                    } else {
                                        // Get UTC timestamp
                                        let utc_dt = match arr.data_type() {
                                            ArrowDataType::Timestamp(TimeUnit::Second, _) => {
                                                let ts_arr = arr.as_primitive::<arrow_array::types::TimestampSecondType>();
                                                DateTime::from_timestamp(ts_arr.value(idx), 0)
                                            }
                                            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                                                let ts_arr = arr.as_primitive::<arrow_array::types::TimestampMillisecondType>();
                                                DateTime::from_timestamp_millis(ts_arr.value(idx))
                                            }
                                            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
                                                let ts_arr = arr.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
                                                DateTime::from_timestamp_micros(ts_arr.value(idx))
                                            }
                                            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
                                                let ts_arr = arr.as_primitive::<arrow_array::types::TimestampNanosecondType>();
                                                Some(DateTime::from_timestamp_nanos(
                                                    ts_arr.value(idx),
                                                ))
                                            }
                                            _ => unreachable!(),
                                        };

                                        let utc_dt = utc_dt.ok_or_else(|| {
                                            ArrowError::ComputeError(
                                                "Invalid timestamp".to_string(),
                                            )
                                        })?;

                                        // Convert to target timezone if specified
                                        let formatted = if let Some(tz) = parsed_tz {
                                            let tz_dt = utc_dt.with_timezone(&tz);
                                            safe_datetime_format(&tz_dt, fmt)?
                                        } else {
                                            safe_datetime_format(&utc_dt, fmt)?
                                        };

                                        builder.append_value(formatted);
                                    }
                                }
                                new_cols.push(Arc::new(builder.finish()) as ArrayRef);
                                new_fields.push(ArrowField::new(
                                    field.name(),
                                    ArrowDataType::LargeUtf8,
                                    field.is_nullable(),
                                ));
                            } else {
                                new_fields.push(field.clone());
                                new_cols.push(batch.column(i).clone());
                            }
                        }
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
                let rb2 = transform_batch(rb.clone(), date_fmt.as_ref(), timestamp_fmt.as_ref())
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
