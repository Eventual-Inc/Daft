use std::{convert::TryFrom, fs, path::PathBuf, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use daft_compression::CompressionCodec;
use daft_csv::CsvValidator;
use daft_io::{GetRange, SourceType, parse_url, strip_file_uri_to_path};
use url::Url;
use urlencoding::decode;

use crate::{ChunkSpec, DataSource, ScanTask, ScanTaskRef, StorageConfig};

type BoxScanTaskIter<'a> = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'a>;
const CSV_SUFFIXES: &[&str] = &[".csv", ".tsv"];

#[must_use]
pub fn split_by_csv_ranges<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    cfg: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
    Box::new(
        scan_tasks
            .map(move |t| -> DaftResult<BoxScanTaskIter<'a>> {
                let t = t?;
                if let (FileFormatConfig::Csv(csv_cfg), [source], Some(None)) = (
                    t.file_format_config.as_ref(),
                    &t.sources[..],
                    t.sources.first().map(DataSource::get_chunk_spec),
                ) {
                    let path = source.get_path();
                    if !supports_csv_split(path) {
                        return Ok(Box::new(std::iter::once(Ok(t))));
                    }

                    let size_bytes =
                        resolve_source_size(path, source.get_size_bytes(), &t.storage_config)?;

                    if size_bytes <= cfg.scan_tasks_max_size_bytes {
                        return Ok(Box::new(std::iter::once(Ok(t))));
                    }

                    let (io_runtime, io_client) = t.storage_config.get_io_client_and_runtime()?;

                    // We need the number of fields to validate CSV record boundaries.
                    // Use the schema from the scan task.
                    let num_fields = t.schema.len();
                    let delimiter = csv_cfg
                        .delimiter
                        .and_then(|c| u8::try_from(c).ok())
                        .unwrap_or(b',');
                    let quote_char = csv_cfg
                        .quote
                        .and_then(|c| u8::try_from(c).ok())
                        .unwrap_or(b'"');
                    let escape_char = csv_cfg.escape_char.and_then(|c| u8::try_from(c).ok());
                    let double_quote = csv_cfg.double_quote;

                    // Align helper: search forward for a valid CSV record boundary (newline not inside quotes).
                    let align_right = |mut pos: usize| -> DaftResult<usize> {
                        if pos >= size_bytes {
                            return Ok(size_bytes);
                        }
                        let mut window = 4096usize;
                        let mut validator = CsvValidator::new(
                            num_fields,
                            quote_char,
                            delimiter,
                            escape_char,
                            double_quote,
                        );
                        loop {
                            let probe_end = (pos + window).min(size_bytes);
                            let bytes = io_runtime.block_on_current_thread(async {
                                let resp = io_client
                                    .single_url_get(
                                        path.to_string(),
                                        Some(GetRange::Bounded(pos..probe_end)),
                                        None,
                                    )
                                    .await?;
                                resp.bytes().await
                            })?;
                            let slice = bytes.as_ref();

                            // Search for valid CSV record boundaries in this window.
                            let mut search_pos = 0;
                            while let Some(rel) =
                                slice[search_pos..].iter().position(|&b| b == b'\n')
                            {
                                let newline_abs = search_pos + rel;
                                let candidate = pos + newline_abs + 1;
                                if candidate >= size_bytes {
                                    return Ok(size_bytes);
                                }

                                // Validate that the bytes after this newline form a valid CSV record.
                                let remaining = &slice[newline_abs + 1..];
                                let result = validator.validate_record(&mut remaining.iter());
                                match result {
                                    Some(true) => {
                                        // Found a valid record boundary.
                                        return Ok(candidate);
                                    }
                                    Some(false) | None => {
                                        // Not a valid boundary (newline was inside a quoted field
                                        // or we couldn't determine validity within this window).
                                        // Try the next newline.
                                        search_pos = newline_abs + 1;
                                    }
                                }
                            }

                            if probe_end == size_bytes {
                                return Ok(size_bytes);
                            }
                            window = window.saturating_mul(2).min(1 << 20);
                            pos = probe_end;
                        }
                    };

                    // Build chunk boundaries.
                    let mut offsets = vec![0usize];
                    let mut pos = 0usize;
                    while pos < size_bytes {
                        let target = pos
                            .checked_add(cfg.scan_tasks_max_size_bytes)
                            .unwrap_or(size_bytes);
                        let end = align_right(target.min(size_bytes))?;
                        if end <= pos {
                            return Err(DaftError::InternalError(format!(
                                "CSV split align_right did not advance: pos={pos}, end={end}"
                            )));
                        }
                        offsets.push(end);
                        pos = end;
                    }
                    if *offsets.last().unwrap() != size_bytes {
                        offsets.push(size_bytes);
                    }

                    let has_headers = csv_cfg.has_headers;

                    let mut new_tasks: Vec<DaftResult<ScanTaskRef>> =
                        Vec::with_capacity(offsets.len().saturating_sub(1));
                    for w in offsets.windows(2) {
                        let start = w[0];
                        let end = w[1];
                        if end <= start {
                            return Err(DaftError::InternalError(format!(
                                "Invalid chunk range: start={start}, end={end}"
                            )));
                        }
                        let mut new_source = source.clone();
                        if let DataSource::File { chunk_spec, .. } = &mut new_source {
                            *chunk_spec = Some(ChunkSpec::Bytes { start, end });
                        }

                        // For non-first chunks, override has_headers to false since the header
                        // is only in the first chunk.
                        let file_format_config = if start > 0 && has_headers {
                            let mut new_csv_cfg = csv_cfg.clone();
                            new_csv_cfg.has_headers = false;
                            Arc::new(FileFormatConfig::Csv(new_csv_cfg))
                        } else {
                            t.file_format_config.clone()
                        };

                        let new_task = ScanTask::new(
                            vec![new_source],
                            file_format_config,
                            t.schema.clone(),
                            t.storage_config.clone(),
                            t.pushdowns.clone(),
                            t.generated_fields.clone(),
                        );
                        new_tasks.push(Ok(new_task.into()));
                    }

                    Ok(Box::new(new_tasks.into_iter()))
                } else {
                    Ok(Box::new(std::iter::once(Ok(t))))
                }
            })
            .flat_map(|t| t.unwrap_or_else(|e| Box::new(std::iter::once(Err(e))))),
    )
}

fn supports_csv_split(path: &str) -> bool {
    if CompressionCodec::from_uri(path).is_some() {
        return false;
    }

    let normalized = decode_path_component(strip_url_params(path));
    CSV_SUFFIXES
        .iter()
        .any(|suffix| ends_with_ignore_ascii_case(&normalized, suffix))
}

fn resolve_source_size(
    path: &str,
    size_hint: Option<u64>,
    storage_config: &StorageConfig,
) -> DaftResult<usize> {
    if let Some(size) = size_hint {
        return Ok(usize::try_from(size).unwrap_or(0));
    }

    if let Some(local_path) = local_path_from_uri(path) {
        let metadata = fs::metadata(&local_path).map_err(DaftError::from)?;
        let file_size = metadata.len();
        return Ok(usize::try_from(file_size).unwrap_or(0));
    }

    let (io_runtime, io_client) = storage_config.get_io_client_and_runtime()?;
    let size = io_runtime.block_on_current_thread(async {
        io_client.single_url_get_size(path.to_string(), None).await
    })?;
    Ok(size)
}

fn local_path_from_uri(path: &str) -> Option<PathBuf> {
    if let Ok((SourceType::File, resolved)) = parse_url(path) {
        let owned = resolved.into_owned();
        let clean_path = strip_url_params(&owned);

        if let Ok(url) = Url::parse(clean_path)
            && let Ok(path_buf) = url.to_file_path()
        {
            return Some(path_buf);
        }

        let stripped = strip_file_uri_to_path(clean_path)?;
        return Some(PathBuf::from(decode_path_component(stripped)));
    }

    if !path.contains("://") {
        return Some(PathBuf::from(decode_path_component(path)));
    }

    None
}

fn decode_path_component(value: &str) -> String {
    decode(value)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| value.to_string())
}

fn strip_url_params(path: &str) -> &str {
    let without_query = path.split_once('?').map_or(path, |(prefix, _)| prefix);
    without_query
        .split_once('#')
        .map_or(without_query, |(prefix, _)| prefix)
}

fn ends_with_ignore_ascii_case(value: &str, suffix: &str) -> bool {
    value
        .to_ascii_lowercase()
        .ends_with(&suffix.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use common_file_formats::CsvSourceConfig;

    use super::*;
    use crate::{ScanTask, StorageConfig};

    fn make_csv_scan_task(path: &str, size_bytes: u64) -> ScanTask {
        ScanTask::new(
            vec![DataSource::File {
                path: path.to_string(),
                chunk_spec: None,
                size_bytes: Some(size_bytes),
                iceberg_delete_files: None,
                metadata: None,
                partition_spec: None,
                statistics: None,
                parquet_metadata: None,
            }],
            Arc::new(FileFormatConfig::Csv(CsvSourceConfig {
                delimiter: None,
                has_headers: true,
                double_quote: true,
                quote: None,
                escape_char: None,
                comment: None,
                allow_variable_columns: false,
                buffer_size: None,
                chunk_size: None,
            })),
            Arc::new(daft_schema::schema::Schema::new(vec![
                daft_schema::field::Field::new("a", daft_schema::dtype::DataType::Int64),
                daft_schema::field::Field::new("b", daft_schema::dtype::DataType::Utf8),
            ])),
            StorageConfig::default().into(),
            crate::Pushdowns::default(),
            None,
        )
    }

    #[test]
    fn test_compressed_csv_not_split() {
        let st = make_csv_scan_task("file:///tmp/f.csv.gz", 10_000_000);
        let cfg = DaftExecutionConfig::default();
        let iter = split_by_csv_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let out = iter.collect::<Vec<_>>();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_small_csv_not_split() {
        let st = make_csv_scan_task("file:///tmp/small.csv", 100);
        let cfg = DaftExecutionConfig::default();
        let iter = split_by_csv_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let out = iter.collect::<Vec<_>>();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_csv_chunk_boundaries_align_to_record_boundaries() {
        let mut payload = String::new();
        payload.push_str("a,b\n");
        for i in 0..100 {
            payload.push_str(&format!("{i},item_{i}\n"));
        }

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let file_path = env::temp_dir().join(format!("daft-csv-boundary-{unique}.csv"));
        fs::write(&file_path, payload.as_bytes()).unwrap();
        let uri = url::Url::from_file_path(&file_path).unwrap().to_string();
        let size_bytes = payload.len() as u64;

        let st = make_csv_scan_task(&uri, size_bytes);
        let mut cfg = DaftExecutionConfig::default();
        cfg.scan_tasks_max_size_bytes = 256;
        cfg.scan_tasks_min_size_bytes = 0;
        let iter = split_by_csv_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let tasks = iter.collect::<Vec<_>>();

        assert!(
            tasks.len() > 1,
            "Expected multiple tasks, got {}",
            tasks.len()
        );

        let bytes = payload.as_bytes();
        let is_newline_boundary = |pos: usize| -> bool {
            pos == 0 || pos == bytes.len() || (pos <= bytes.len() && bytes[pos - 1] == b'\n')
        };

        for t in &tasks {
            let t = t.as_ref().unwrap();
            let src = &t.sources[0];
            if let crate::DataSource::File {
                chunk_spec: Some(crate::ChunkSpec::Bytes { start, end }),
                ..
            } = src
            {
                assert!(
                    end > start,
                    "Empty chunk or reversed boundary: start={start}, end={end}"
                );
                assert!(
                    is_newline_boundary(*start),
                    "start is not a newline boundary: {start}"
                );
                assert!(
                    is_newline_boundary(*end),
                    "end is not a newline boundary: {end}"
                );
            } else {
                panic!("Expected Bytes chunk_spec");
            }
        }

        // Verify that first task has has_headers=true and subsequent tasks have has_headers=false
        for (i, t) in tasks.iter().enumerate() {
            let t = t.as_ref().unwrap();
            if let FileFormatConfig::Csv(cfg) = t.file_format_config.as_ref() {
                if i == 0 {
                    assert!(cfg.has_headers, "First task should have has_headers=true");
                } else {
                    assert!(!cfg.has_headers, "Task {i} should have has_headers=false");
                }
            }
        }

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_csv_split_with_quoted_newlines() {
        let mut payload = String::new();
        payload.push_str("a,b\n");
        for i in 0..50 {
            payload.push_str(&format!("{i},\"line1\nline2\"\n"));
        }

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let file_path = env::temp_dir().join(format!("daft-csv-quoted-{unique}.csv"));
        fs::write(&file_path, payload.as_bytes()).unwrap();
        let uri = url::Url::from_file_path(&file_path).unwrap().to_string();
        let size_bytes = payload.len() as u64;

        let st = make_csv_scan_task(&uri, size_bytes);
        let mut cfg = DaftExecutionConfig::default();
        cfg.scan_tasks_max_size_bytes = 128;
        cfg.scan_tasks_min_size_bytes = 0;
        let iter = split_by_csv_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let tasks: Vec<_> = iter.collect();

        assert!(tasks.len() > 1, "Expected multiple tasks");

        // Verify all boundaries fall on valid record boundaries (not inside quoted fields)
        let bytes = payload.as_bytes();
        for t in &tasks {
            let t = t.as_ref().unwrap();
            let src = &t.sources[0];
            if let crate::DataSource::File {
                chunk_spec: Some(crate::ChunkSpec::Bytes { start, end }),
                ..
            } = src
            {
                // The byte at start should be the beginning of a new record
                // (i.e. the previous byte should be \n or start == 0)
                assert!(
                    *start == 0 || bytes[*start - 1] == b'\n',
                    "start {start} is not at a record boundary"
                );
                assert!(
                    *end == bytes.len() || bytes[*end - 1] == b'\n',
                    "end {end} is not at a record boundary"
                );
            }
        }

        fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_supports_csv_split_extension_rules() {
        assert!(supports_csv_split("file:///tmp/data.csv"));
        assert!(supports_csv_split("s3://bucket/data.CSV"));
        assert!(supports_csv_split("file:///tmp/data.tsv"));
        assert!(!supports_csv_split("file:///tmp/data.csv.gz"));
        assert!(!supports_csv_split("file:///tmp/data.json"));
        assert!(!supports_csv_split("file:///tmp/data.parquet"));
    }
}
