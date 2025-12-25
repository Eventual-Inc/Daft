use std::{convert::TryFrom, fs, path::PathBuf};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, JsonSourceConfig};
use daft_compression::CompressionCodec;
use daft_io::{GetRange, SourceType, parse_url, strip_file_uri_to_path};
use url::Url;
use urlencoding::decode;

use crate::{ChunkSpec, DataSource, ScanTask, ScanTaskRef, StorageConfig};

type BoxScanTaskIter<'a> = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'a>;
const JSONL_SUFFIXES: &[&str] = &[".jsonl", ".ndjson"];
const JSON_SUFFIXES: &[&str] = &[".json"];

#[must_use]
pub fn split_by_jsonl_ranges<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    cfg: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
    Box::new(
        scan_tasks
            .map(move |t| -> DaftResult<BoxScanTaskIter<'a>> {
                let t = t?;
                if let (FileFormatConfig::Json(JsonSourceConfig { .. }), [source], Some(None)) = (
                    t.file_format_config.as_ref(),
                    &t.sources[..],
                    t.sources.first().map(DataSource::get_chunk_spec),
                ) {
                    let path = source.get_path();
                    if !supports_split(path) {
                        return Ok(Box::new(std::iter::once(Ok(t))));
                    }

                    // Determine file size; prefer cached size, else fetch.
                    let size_bytes =
                        resolve_source_size(path, source.get_size_bytes(), &t.storage_config)?;

                    if size_bytes <= cfg.scan_tasks_max_size_bytes {
                        return Ok(Box::new(std::iter::once(Ok(t))));
                    }

                    let (io_runtime, io_client) = t.storage_config.get_io_client_and_runtime()?;

                    // Align helpers: search forwards for newline, expanding window until found or at bounds.
                    let align_right = |mut pos: usize| -> DaftResult<usize> {
                        if pos >= size_bytes {
                            return Ok(size_bytes);
                        }
                        let mut window = 4096usize;
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
                            if let Some(rel) = slice.iter().position(|&b| b == b'\n') {
                                return Ok(pos + rel + 1);
                            }
                            if probe_end == size_bytes {
                                return Ok(size_bytes);
                            }
                            window = window.saturating_mul(2).min(1 << 20);
                            pos = probe_end;
                        }
                    };

                    // Build chunk boundaries by advancing forward and aligning end positions to the next newline.
                    // This avoids pathological cases where the first cut lies within the first (very long) line.
                    let mut offsets = vec![0usize];
                    let mut pos = 0usize;
                    while pos < size_bytes {
                        // Use checked_add to avoid usize overflow on large positions
                        let target = pos
                            .checked_add(cfg.scan_tasks_max_size_bytes)
                            .unwrap_or(size_bytes);
                        let end = align_right(target.min(size_bytes))?;
                        // Invariant: aligned end should always advance beyond current position
                        assert!(
                            end > pos,
                            "align_right did not advance: pos={pos}, end={end}"
                        );
                        offsets.push(end);
                        pos = end;
                    }
                    if *offsets.last().unwrap() != size_bytes {
                        offsets.push(size_bytes);
                    }

                    // Preallocate capacity for tasks to avoid reallocations
                    let mut new_tasks: Vec<DaftResult<ScanTaskRef>> =
                        Vec::with_capacity(offsets.len().saturating_sub(1));
                    for w in offsets.windows(2) {
                        let start = w[0];
                        let end = w[1];
                        assert!(end > start, "Invalid chunk range: start={start}, end={end}");
                        let mut new_source = source.clone();
                        if let DataSource::File { chunk_spec, .. } = &mut new_source {
                            *chunk_spec = Some(ChunkSpec::Bytes { start, end });
                        }
                        let new_task = ScanTask::new(
                            vec![new_source],
                            t.file_format_config.clone(),
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

fn supports_split(path: &str) -> bool {
    // TODO: Support compressed JSONL files (gzip, bzip2, etc.)
    if CompressionCodec::from_uri(path).is_some() {
        return false;
    }

    let normalized = decode_path_component(strip_url_params(path));

    if JSON_SUFFIXES
        .iter()
        .any(|suffix| ends_with_ignore_ascii_case(&normalized, suffix))
    {
        return false;
    }

    JSONL_SUFFIXES
        .iter()
        .any(|suffix| ends_with_ignore_ascii_case(&normalized, suffix))
}

fn resolve_source_size(
    path: &str,
    size_hint: Option<u64>,
    storage_config: &StorageConfig,
) -> DaftResult<usize> {
    if let Some(size) = size_hint {
        // On overflow converting to usize, treat as unknown/untrusted size: return 0 to avoid splitting.
        return Ok(usize::try_from(size).unwrap_or(0));
    }

    if let Some(local_path) = local_path_from_uri(path) {
        let metadata = fs::metadata(&local_path).map_err(DaftError::from)?;
        let file_size = metadata.len();
        // On overflow, treat as unknown size: return 0 (no split) to avoid pathological loops.
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

        // Strip query params and fragments before processing
        let clean_path = strip_url_params(&owned);

        if let Ok(url) = Url::parse(clean_path)
            && let Ok(path_buf) = url.to_file_path()
        {
            return Some(path_buf);
        }

        // Fallback for when Url::to_file_path() fails
        // parse_url always returns file paths with "file://" prefix, so this should always succeed
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

    use common_file_formats::JsonSourceConfig;

    use super::*;
    use crate::{ScanTask, StorageConfig};

    fn make_scan_task(path: &str, size_bytes: u64) -> ScanTask {
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
            Arc::new(FileFormatConfig::Json(JsonSourceConfig::default())),
            Arc::new(daft_schema::schema::Schema::empty()),
            StorageConfig::default().into(),
            crate::Pushdowns::default(),
            None,
        )
    }

    #[test]
    fn test_offsets_non_empty() {
        let st = make_scan_task("file:///dev/null.jsonl", 10_000_000);
        let cfg = common_daft_config::DaftExecutionConfig::default();
        let iter = split_by_jsonl_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let out = iter.collect::<Vec<_>>();
        // Should have exactly 1 task since input is a single task with .jsonl extension
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_chunk_boundaries_align_to_newlines() {
        // Construct a JSONL payload with an extremely long first line and irregular line lengths
        let mut payload = String::new();
        // Extremely long first line (> 1MB window would expand; here we only simulate the logic and avoid actual IO)
        payload.push_str(&format!("{{\"id\":0,\"val\":\"{}\"}}\n", "x".repeat(1024)));
        // Several irregular line lengths
        for i in 1..50 {
            let len = (i * 13) % 257; // irregular length
            payload.push_str(&format!(
                "{{\"id\":{},\"val\":\"{}\"}}\n",
                i,
                "y".repeat(len)
            ));
        }

        // Write to a temporary file to obtain size_bytes
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let file_path = std::env::temp_dir().join(format!("daft-jsonl-boundary-{unique}.jsonl"));
        std::fs::write(&file_path, payload.as_bytes()).unwrap();
        let uri = url::Url::from_file_path(&file_path).unwrap().to_string();
        let size_bytes = payload.len() as u64;

        // Build a ScanTask and run split_by_jsonl_ranges
        let st = make_scan_task(&uri, size_bytes);
        let mut cfg = common_daft_config::DaftExecutionConfig::default();
        // Lower thresholds to force splitting
        cfg.scan_tasks_max_size_bytes = 4 * 1024; // 4KB
        cfg.scan_tasks_min_size_bytes = 0;
        let iter = split_by_jsonl_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let tasks = iter.collect::<Vec<_>>();

        // Extract and verify all boundaries
        let bytes = payload.as_bytes();
        let is_newline_boundary = |pos: usize| -> bool {
            pos == 0 || pos == bytes.len() || (pos <= bytes.len() && bytes[pos - 1] == b'\n')
        };

        for t in tasks {
            let t = t.unwrap();
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

        // Clean up temporary file
        std::fs::remove_file(&file_path).unwrap();
    }

    #[test]
    fn test_compressed_not_split() {
        let st = make_scan_task("file:///tmp/f.jsonl.gz", 10_000_000);
        let cfg = common_daft_config::DaftExecutionConfig::default();
        let iter = split_by_jsonl_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let out = iter.collect::<Vec<_>>();
        // Compressed files should not be split, exactly 1 task
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_json_file_not_split() {
        let st = make_scan_task("file:///tmp/data.json", 10_000_000);
        let cfg = common_daft_config::DaftExecutionConfig::default();
        let iter = split_by_jsonl_ranges(Box::new(std::iter::once(Ok(Arc::new(st).into()))), &cfg);
        let out = iter.collect::<Vec<_>>();
        // .json files should not be split, exactly 1 task
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn test_split_by_jsonl_ranges_multiple_tasks() {
        let st1 = make_scan_task("file:///tmp/file1.jsonl", 10_000_000); // 10MB file
        let st2 = make_scan_task("file:///tmp/file2.jsonl", 10_000_000); // 10MB file
        let st3 = make_scan_task("file:///tmp/file3.jsonl", 10_000_000); // 10MB file

        let cfg = common_daft_config::DaftExecutionConfig::default();
        let iter = split_by_jsonl_ranges(
            Box::new(
                vec![
                    Ok(Arc::new(st1).into()),
                    Ok(Arc::new(st2).into()),
                    Ok(Arc::new(st3).into()),
                ]
                .into_iter(),
            ),
            &cfg,
        );

        let out = iter.collect::<Vec<_>>();
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn test_supports_split_extension_rules() {
        assert!(super::supports_split("file:///tmp/data.jsonl"));
        assert!(super::supports_split("s3://bucket/data.NDJSON"));
        assert!(!super::supports_split("file:///tmp/data.json"));
        assert!(!super::supports_split("file:///tmp/data.jsonl.gz"));
    }

    #[test]
    fn test_resolve_source_size_local_path() {
        let payload = b"{\"a\":1}\n{\"a\":2}\n";
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let file_path = env::temp_dir().join(format!("daft-jsonl-split-{unique}.jsonl"));
        fs::write(&file_path, payload).unwrap();
        let uri = Url::from_file_path(&file_path)
            .expect("Failed to create file:// URL from path")
            .to_string();
        let storage = StorageConfig::default();
        let size = super::resolve_source_size(&uri, None, &storage).unwrap();
        assert_eq!(size, payload.len());
        fs::remove_file(&file_path).unwrap();
    }

    // Tests for decode_path_component
    #[test]
    fn test_decode_path_component_plain() {
        assert_eq!(decode_path_component("foo/bar"), "foo/bar");
    }

    #[test]
    fn test_decode_path_component_encoded() {
        assert_eq!(decode_path_component("foo%20bar"), "foo bar");
        assert_eq!(decode_path_component("foo%2Fbar"), "foo/bar");
    }

    // Tests for strip_url_params
    #[test]
    fn test_strip_url_params_no_query_no_fragment() {
        assert_eq!(
            strip_url_params("file:///path/to/file"),
            "file:///path/to/file"
        );
    }

    #[test]
    fn test_strip_url_params_with_query() {
        assert_eq!(
            strip_url_params("file:///path/to/file?foo=bar"),
            "file:///path/to/file"
        );
    }

    #[test]
    fn test_strip_url_params_with_fragment() {
        assert_eq!(
            strip_url_params("file:///path/to/file#section"),
            "file:///path/to/file"
        );
    }

    #[test]
    fn test_strip_url_params_with_both() {
        assert_eq!(
            strip_url_params("file:///path/to/file?foo=bar#section"),
            "file:///path/to/file"
        );
    }

    // Tests for ends_with_ignore_ascii_case
    #[test]
    fn test_ends_with_ignore_ascii_case_exact() {
        assert!(ends_with_ignore_ascii_case("file.jsonl", ".jsonl"));
    }

    #[test]
    fn test_ends_with_ignore_ascii_case_different_case() {
        assert!(ends_with_ignore_ascii_case("file.JSONL", ".jsonl"));
        assert!(ends_with_ignore_ascii_case("file.JsonL", ".JSONL"));
        assert!(ends_with_ignore_ascii_case("file.ndjson", ".NDJSON"));
    }

    #[test]
    fn test_ends_with_ignore_ascii_case_no_match() {
        assert!(!ends_with_ignore_ascii_case("file.json", ".jsonl"));
        assert!(!ends_with_ignore_ascii_case("file.jsonl.gz", ".jsonl"));
    }

    // Tests for local_path_from_uri
    #[test]
    fn test_local_path_from_uri_plain_path() {
        assert_eq!(
            local_path_from_uri("/tmp/file"),
            Some(PathBuf::from("/tmp/file"))
        );
    }

    #[test]
    fn test_local_path_from_uri_file_url() {
        let result = local_path_from_uri("file:///tmp/file").unwrap();
        // URL parsing handles this correctly
        assert!(
            result.to_string_lossy().contains("tmp") || result.to_string_lossy().contains("/tmp")
        );
    }

    #[test]
    fn test_local_path_from_uri_with_query() {
        let result = local_path_from_uri("file:///tmp/file?foo=bar").unwrap();
        // Should strip query params
        assert!(!result.to_string_lossy().contains("?"));
    }

    #[test]
    fn test_resolve_source_size_size_hint_overflow_fallback() {
        // When size_hint is extremely large, conversion to usize may overflow on 32-bit.
        // In such cases, resolve_source_size should fall back to 0 (no split). On 64-bit it fits.
        let storage = StorageConfig::default();
        let path = "file:///tmp/overflow.jsonl";
        let res = resolve_source_size(path, Some(u64::MAX), &storage).unwrap();
        #[cfg(target_pointer_width = "32")]
        assert_eq!(res, 0);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(res, usize::MAX);
    }
}
