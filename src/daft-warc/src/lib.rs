use std::{io::BufReader, sync::Arc};

use arrow2::{self, array::BinaryArray};
use common_error::DaftResult;
use daft_core::{prelude::{Field, Schema, SchemaRef, TimeUnit}, series::Series};
use daft_dsl::ExprRef;
use daft_io::{parse_url, IOClient, IOStatsRef, SourceType};
use daft_recordbatch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use warc::{BufferedBody, Record, WarcHeader, WarcReader, Error as WarcError};
use arrow2::array::{Int64Array, Utf8Array};

pub struct WarcConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub schema: Option<SchemaRef>,
    pub predicate: Option<ExprRef>,
}

fn get_warc_reader(uri: &str, file: std::fs::File) -> WarcReader<BufReader<Box<dyn std::io::Read>>> {
    let reader = if uri.ends_with(".gz") {
        let gzip_reader = flate2::read::GzDecoder::new(file);
        Box::new(gzip_reader) as Box<dyn std::io::Read>
    } else {
        Box::new(file) as Box<dyn std::io::Read>
    };
    let buf_reader = BufReader::with_capacity(16 * 1024 * 1024, reader);
    WarcReader::new(buf_reader)
}

struct WarcChunkIterator<I> {
    warc_reader: I,
    current_size: usize,
    records: Vec<Record<BufferedBody>>,
}

impl<I> Iterator for WarcChunkIterator<I>
where
    I: Iterator<Item = Result<Record<BufferedBody>, WarcError>>,
{
    type Item = Vec<Record<BufferedBody>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(record) = self.warc_reader.next() {
            match record {
                Ok(record) => {
                    self.current_size += record.content_length() as usize;
                    self.records.push(record);
                    if self.current_size >= 1 * 1024 * 1024 {
                        let batch_records = std::mem::take(&mut self.records);
                        self.current_size = 0;
                        return Some(batch_records);
                    }
                }
                Err(e) => {
                    eprintln!("Error reading WARC record: {}", e);
                }
            }
        }
        if !self.records.is_empty() {
            let batch_records = std::mem::take(&mut self.records);
            self.current_size = 0;
            Some(batch_records)
        } else {
            None
        }
    }
}

impl<I> WarcChunkIterator<I> {
    fn new(warc_reader: I) -> Self {
        Self { warc_reader, current_size: 0, records: Vec::new() }
    }
}

async fn stream_warc_local(uri: &str, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let uri = uri.trim_start_matches("file://").to_string();
    let (tx, rx): (
        tokio::sync::mpsc::Sender<DaftResult<RecordBatch>>,
        tokio::sync::mpsc::Receiver<DaftResult<RecordBatch>>
    ) = tokio::sync::mpsc::channel(20);
    let schema = if let Some(schema) = convert_options.schema {
        schema
    } else {
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("warc_id", daft_core::prelude::DataType::Utf8),
            Field::new("warc_type", daft_core::prelude::DataType::Utf8),
            Field::new("warc_version", daft_core::prelude::DataType::Utf8),
            Field::new("warc_date", daft_core::prelude::DataType::Timestamp(TimeUnit::Nanoseconds, Some("Etc/UTC".to_string()))),
            Field::new("warc_target_uri", daft_core::prelude::DataType::Utf8),
            Field::new("warc_content_type", daft_core::prelude::DataType::Utf8),
            Field::new("warc_content_length", daft_core::prelude::DataType::Int64),
            Field::new("warc_body", daft_core::prelude::DataType::Binary),
        ])?);
        full_schema
    };
    let (schema, projection_indices) = if let Some(include_columns) = &convert_options.include_columns {
        let mut indices = Vec::new();
        let filtered_fields: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| include_columns.contains(name))
            .map(|(i, (_, field))| {
                indices.push(i);
                field.clone()
            })
            .collect();
        (Arc::new(Schema::new(filtered_fields)?), Some(indices))
    } else {
        (schema, None)
    };

    let file = std::fs::File::open(&uri).unwrap();
    let warc_reader = get_warc_reader(&uri, file);
    let chunk_iter = WarcChunkIterator::new(warc_reader.iter_records());
    let stream = futures::stream::iter(chunk_iter).map(move |records| {
        create_record_batch(records, schema.clone(), projection_indices.as_ref())
    });

    // TODO: apply limit.
    Ok(Box::pin(futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|result| match result {
                Ok(batch) => (Ok(batch), rx),
                Err(e) => (Err(e.into()), rx)
            })
    })))
}

fn create_record_batch(
    records: Vec<Record<BufferedBody>>,
    schema: SchemaRef,
    projection_indices: Option<&Vec<usize>>,
) -> DaftResult<RecordBatch> {
    let num_records = records.len();
    
    // Only allocate vectors for fields we need
    let mut warc_types = if projection_indices.map_or(true, |indices| indices.contains(&1)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_ids = if projection_indices.map_or(true, |indices| indices.contains(&0)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_versions = if projection_indices.map_or(true, |indices| indices.contains(&2)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_dates = if projection_indices.map_or(true, |indices| indices.contains(&3)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_target_uris = if projection_indices.map_or(true, |indices| indices.contains(&4)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_content_types = if projection_indices.map_or(true, |indices| indices.contains(&5)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_content_lengths = if projection_indices.map_or(true, |indices| indices.contains(&6)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };
    let mut warc_bodies = if projection_indices.map_or(true, |indices| indices.contains(&7)) {
        Some(Vec::with_capacity(records.len()))
    } else {
        None
    };

    for record in records {
        if let Some(types) = warc_types.as_mut() {
            types.push(record.warc_type().to_string());
        }
        if let Some(ids) = warc_ids.as_mut() {
            ids.push(record.warc_id().to_string());
        }
        if let Some(versions) = warc_versions.as_mut() {
            versions.push(record.warc_version().to_string());
        }
        if let Some(dates) = warc_dates.as_mut() {
            dates.push(record.date().timestamp() as i64);
        }
        if let Some(target_uris) = warc_target_uris.as_mut() {
            target_uris.push(record.header(WarcHeader::TargetURI).unwrap_or_default().to_string());
        }
        if let Some(content_types) = warc_content_types.as_mut() {
            content_types.push(record.header(WarcHeader::ContentType).unwrap_or_default().to_string());
        }
        if let Some(content_lengths) = warc_content_lengths.as_mut() {
            content_lengths.push(record.content_length() as i64);
        }
        if let Some(bodies) = warc_bodies.as_mut() {
            bodies.push(record.body().to_vec());
        }
    }

    // Create series and return RecordBatch
    let mut series_vec = Vec::with_capacity(schema.fields.len());
    for (idx, (_, field)) in schema.fields.iter().enumerate() {
        let orig_idx = projection_indices.map_or(idx, |indices| indices[idx]);
        
        let series = match orig_idx {
            0 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_ids.as_ref().unwrap().as_slice())))?,
            1 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_types.as_ref().unwrap().as_slice())))?,
            2 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_versions.as_ref().unwrap().as_slice())))?,
            3 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Int64Array::from_slice(warc_dates.as_ref().unwrap().as_slice())))?,
            4 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_target_uris.as_ref().unwrap().as_slice())))?,
            5 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Utf8Array::<i64>::from_slice(warc_content_types.as_ref().unwrap().as_slice())))?,
            6 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(Int64Array::from_slice(warc_content_lengths.as_ref().unwrap().as_slice())))?,
            7 => Series::from_arrow(Arc::new(field.clone()), 
                Box::new(BinaryArray::<i64>::from_slice(warc_bodies.as_ref().unwrap().as_slice())))?,
            _ => unreachable!(),
        };
        series_vec.push(series);
    }

    RecordBatch::new_with_size(
        schema.clone(),
        series_vec,
        num_records,
    )
}

pub async fn stream_warc(uri: String, io_client: Arc<IOClient>, io_stats: IOStatsRef, convert_options: WarcConvertOptions) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let uri = uri.as_str();
    let (source_type, _) = parse_url(uri)?;
    if matches!(source_type, SourceType::File) {
        stream_warc_local(uri, convert_options).await
    } else {
        todo!()
    }
}
