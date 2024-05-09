use std::{borrow::Cow, collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_table::Table;
use indexmap::IndexMap;
use num_traits::Pow;

use rayon::prelude::*;
use serde_json::value::RawValue;
use snafu::ResultExt;

use crate::{
    decoding::{allocate_array, deserialize_into},
    deserializer::Value,
    inference::{column_types_map_to_fields, infer_records_schema},
    read::tables_concat,
    ArrowSnafu, JsonConvertOptions, JsonParseOptions, JsonReadOptions,
};

const NEWLINE: u8 = b'\n';
const CLOSING_BRACKET: u8 = b'}';

pub fn read_json_local(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
) -> DaftResult<Table> {
    let uri = uri.trim_start_matches("file://");
    let file = std::fs::File::open(uri)?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }.expect("TODO");
    let bytes = &mmap[..];
    let reader = JsonReader::try_new(bytes, convert_options, parse_options, read_options)?;
    reader.finish()
}

struct JsonReader<'a> {
    bytes: &'a [u8],
    schema: Schema,
    n_threads: usize,
    sample_size: usize,
    n_rows: Option<usize>,
    chunk_size: Option<usize>,
}

impl<'a> JsonReader<'a> {
    pub fn try_new(
        bytes: &'a [u8],
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
    ) -> DaftResult<Self> {
        let sample_size = parse_options
            .as_ref()
            .and_then(|options| options.sample_size)
            .unwrap_or(1024);
        let n_rows = convert_options.as_ref().and_then(|options| options.limit);
        let chunk_size = read_options.as_ref().and_then(|options| options.chunk_size);

        let schema = match convert_options.and_then(|options| options.schema) {
            Some(schema) => schema.as_ref().clone(),
            None => Schema::try_from(&infer_schema(bytes, None, None)?)?,
        };
        Ok(Self {
            bytes,
            schema,
            n_threads: 6, // todo
            sample_size,
            n_rows,
            chunk_size,
        })
    }

    pub fn finish(&self) -> DaftResult<Table> {
        self.read_json_impl()
    }

    fn read_json_impl(&self) -> DaftResult<Table> {
        let mut bytes = self.bytes;
        let mut n_threads = self.n_threads;
        let mut total_rows = 128;

        if let Some((mean, std)) = get_line_stats_json(bytes, self.sample_size) {
            let line_length_upper_bound = mean + 1.1 * std;

            total_rows = (bytes.len() as f32 / (mean - 0.01 * std)) as usize;
            if let Some(n_rows) = self.n_rows {
                total_rows = std::cmp::min(n_rows, total_rows);
                // the guessed upper bound of the no. of bytes in the file
                let n_bytes = (line_length_upper_bound * (n_rows as f32)) as usize;

                if n_bytes < bytes.len() {
                    if let Some(pos) = next_line_position(&bytes[n_bytes..]) {
                        bytes = &bytes[..n_bytes + pos]
                    }
                }
            }
        }
        if total_rows <= 128 {
            n_threads = 1;
        }

        let total_len = bytes.len();
        let chunk_size = self.chunk_size.unwrap_or_else(|| bytes.len() / n_threads);
        let file_chunks = get_file_chunks(bytes, n_threads, total_len, chunk_size);

        let tbls = file_chunks
            .into_par_iter()
            .map(|(start, stop)| {
                let chunk = &bytes[start..stop];
                parse_json_chunk(chunk, &self.schema, chunk_size)
            })
            .collect::<DaftResult<Vec<Table>>>()?;

        let tbl = tables_concat(tbls)?;

        // The `limit` is not guaranteed, so we need to properly apply the limit after concatenating the tables
        let len = tbl.len();
        if let Some(limit) = self.n_rows {
            if len > limit {
                return tbl.head(limit);
            }
        };
        Ok(tbl)
    }
}

fn infer_schema(
    bytes: &[u8],
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
) -> DaftResult<arrow2::datatypes::Schema> {
    let max_bytes = max_bytes.unwrap_or(1024 * 1024); // todo: make this configurable
    let max_records = max_rows.unwrap_or(1024); // todo: make this configurable

    let mut total_bytes = 0;

    let mut column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>> = IndexMap::new();
    let mut scratch = Vec::new();
    let scratch = &mut scratch;

    let iter =
        serde_json::Deserializer::from_slice(bytes).into_iter::<Box<serde_json::value::RawValue>>();
    for value in iter.take(max_records) {
        let value = value?;
        let bytes = value.get().as_bytes();
        total_bytes += bytes.len();

        let v = parse_raw_value(&value, scratch)?;

        let inferred_schema = infer_records_schema(&v).context(ArrowSnafu)?;
        for field in inferred_schema.fields {
            // Get-and-mutate-or-insert.
            match column_types.entry(field.name) {
                indexmap::map::Entry::Occupied(mut v) => {
                    v.get_mut().insert(field.data_type);
                }
                indexmap::map::Entry::Vacant(v) => {
                    let mut a = HashSet::new();
                    a.insert(field.data_type);
                    v.insert(a);
                }
            }
        }
        scratch.clear();

        if total_bytes > max_bytes {
            break;
        }
    }

    let fields = column_types_map_to_fields(column_types);
    Ok(fields.into())
}

/// Get the mean and standard deviation of length of lines in bytes
fn get_line_stats_json(bytes: &[u8], n_lines: usize) -> Option<(f32, f32)> {
    let mut lengths = Vec::with_capacity(n_lines);

    let mut bytes_trunc;
    let n_lines_per_iter = n_lines / 2;

    let mut n_read = 0;

    let bytes_len = bytes.len();

    // sample from 0, and 75% in the file
    for offset in [0, (bytes_len as f32 * 0.75) as usize] {
        bytes_trunc = &bytes[offset..];
        let pos = next_line_position(bytes_trunc)?;
        if pos >= bytes_len {
            return None;
        }
        bytes_trunc = &bytes_trunc[pos + 1..];

        for _ in offset..(offset + n_lines_per_iter) {
            let pos = next_line_position(bytes_trunc);
            if let Some(pos) = pos {
                lengths.push(pos);
                let next_bytes = &bytes_trunc[pos..];
                if next_bytes.is_empty() {
                    return None;
                }
                bytes_trunc = next_bytes;
                n_read += pos;
            } else {
                break;
            }
        }
    }

    let n_samples = lengths.len();
    let mean = (n_read as f32) / (n_samples as f32);
    let mut std = 0.0;
    for &len in lengths.iter() {
        std += (len as f32 - mean).pow(2.0)
    }
    std = (std / n_samples as f32).sqrt();
    Some((mean, std))
}

/// Calculate the max number of chunks to split the file into
/// It looks for the largest number divisible by `n_threads` and less than `chunk_size`
/// It has an arbitrary limit of `n_threads * n_threads`, which seems to work well in practice.
///
/// Example:
///
/// ```text
/// n_threads = 4
/// chunk_size = 2048
/// calculate_chunks_and_size(n_threads, chunk_size) = (16, 128)
/// ```
fn calculate_chunks_and_size(n_threads: usize, chunk_size: usize, total: usize) -> (usize, usize) {
    let mut max_divisible_chunks = n_threads;

    // The maximum number of chunks is n_threads * n_threads
    // This was chosen based on some crudely done benchmarks. It seems to work well in practice.
    // The idea is to have a number of chunks that is a divisible by the number threads to maximize parallelism.
    // But we dont want to have too small chunks, as that would increase the overhead of the parallelism.
    // This is a heuristic and could be improved.
    let max_chunks = n_threads * n_threads;

    while max_divisible_chunks <= chunk_size && max_divisible_chunks < max_chunks {
        let md = max_divisible_chunks + n_threads;
        if md > chunk_size || md > max_chunks {
            break;
        }
        max_divisible_chunks = md;
    }
    let chunk_size = total / n_threads;
    (max_divisible_chunks, chunk_size)
}

/// Get the start and end positions of the chunks of the file
fn get_file_chunks(
    bytes: &[u8],
    n_threads: usize,
    total_len: usize,
    chunk_size: usize,
) -> Vec<(usize, usize)> {
    let mut last_pos = 0;

    let (n_chunks, chunk_size) = calculate_chunks_and_size(n_threads, chunk_size, total_len);

    let mut offsets = Vec::with_capacity(n_chunks);

    for _ in 0..n_chunks {
        let search_pos = last_pos + chunk_size;

        if search_pos >= bytes.len() {
            break;
        }

        let end_pos = match next_line_position(&bytes[search_pos..]) {
            Some(pos) => search_pos + pos,
            None => {
                break;
            }
        };

        offsets.push((last_pos, end_pos));
        last_pos = end_pos;
    }

    offsets.push((last_pos, total_len));

    offsets
}

#[inline(always)]
fn parse_raw_value<'a>(
    raw_value: &'a RawValue,
    scratch: &'a mut Vec<u8>,
) -> std::result::Result<Value<'a>, super::Error> {
    let bytes = raw_value.get().as_bytes();
    scratch.clear();
    // We need to clone the bytes here because the deserializer expects a mutable slice
    // and `RawValue` only provides an immutable slice.
    scratch.extend_from_slice(bytes);
    crate::deserializer::to_value(scratch).map_err(|e| super::Error::JsonDeserializationError {
        string: e.to_string(),
    })
}

fn parse_json_chunk(bytes: &[u8], schema: &Schema, chunk_size: usize) -> DaftResult<Table> {
    let mut scratch = vec![];
    let scratch = &mut scratch;

    let daft_fields = Arc::new(
        schema
            .fields
            .values()
            .map(|f| Arc::new(f.clone()))
            .collect::<Vec<_>>(),
    );
    let arrow_schema = schema.to_arrow()?;

    // The `RawValue` is a pointer to the original JSON string and does not perform any deserialization.
    // This is a trick to use the line-based deserializer from serde_json to iterate over the lines
    // This is more accurate than using a `Lines` iterator.
    // Ideally, we would instead use a line-based deserializer from simd_json, but that is not available.
    let iter =
        serde_json::Deserializer::from_slice(bytes).into_iter::<Box<serde_json::value::RawValue>>();

    let mut columns = arrow_schema
        .fields
        .iter()
        .map(|f| {
            (
                Cow::Owned(f.name.to_string()),
                allocate_array(f, chunk_size),
            )
        })
        .collect::<IndexMap<_, _>>();

    for record in iter {
        let value = record.map_err(|e| super::Error::JsonDeserializationError {
            string: e.to_string(),
        })?;
        let v = parse_raw_value(&value, scratch)?;

        match v {
            Value::Object(record) => {
                for (s, inner) in columns.iter_mut() {
                    match record.get(s) {
                        Some(value) => {
                            deserialize_into(inner, &[value]);
                        }
                        None => {
                            Err(super::Error::JsonDeserializationError {
                                string: "Field not found in schema".to_string(),
                            })?;
                        }
                    };
                }
            }
            _ => {
                return Err(super::Error::JsonDeserializationError {
                    string: "Expected JSON object".to_string(),
                }
                .into());
            }
        }
    }
    let columns = columns
        .into_values()
        .zip(daft_fields.iter())
        .map(|(mut ma, fld)| {
            let arr = ma.as_box();
            Series::try_from_field_and_arrow_array(fld.clone(), cast_array_for_daft_if_needed(arr))
        })
        .collect::<DaftResult<Vec<_>>>()?;

    Ok(Table::new_unchecked(schema.clone(), columns))
}

pub(crate) fn next_line_position(input: &[u8]) -> Option<usize> {
    let pos = memchr::memchr(NEWLINE, input)?;
    if pos == 0 {
        return Some(1);
    }

    let is_closing_bracket = input.get(pos - 1) == Some(&CLOSING_BRACKET);
    if is_closing_bracket {
        Some(pos + 1)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow2::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };

    #[test]
    fn test_infer_schema() {
        let json = r#"
{"floats": 1.0, "utf8": "hello", "bools": true}
{"floats": 2.0, "utf8": "world", "bools": false}
{"floats": 3.0, "utf8": "!", "bools": true}
"#;

        let result = infer_schema(json.as_bytes(), None, None);
        let expected_schema = ArrowSchema::from(vec![
            ArrowField::new("floats", ArrowDataType::Float64, true),
            ArrowField::new("utf8", ArrowDataType::Utf8, true),
            ArrowField::new("bools", ArrowDataType::Boolean, true),
        ]);
        assert_eq!(result.unwrap(), expected_schema);
    }

    #[test]
    fn test_infer_schema_empty() {
        let json = r#""#;

        let result = infer_schema(json.as_bytes(), None, None);
        let expected_schema = ArrowSchema::from(vec![]);
        assert_eq!(result.unwrap(), expected_schema);
    }

    #[test]
    fn test_read_json_from_str() {
        let json = r#"
{"floats": 1.0, "utf8": "hello", "bools": true}
{"floats": 2.0, "utf8": "world", "bools": false}
{"floats": 3.0, "utf8": "!\\n", "bools": true}
"#;
        let reader = JsonReader::try_new(json.as_bytes(), None, None, None).unwrap();
        let _result = reader.finish();
    }

    #[test]
    fn test_read_json() -> DaftResult<()> {
        let uri = "/Users/corygrinstead/Downloads/stackexchange_sample.jsonl";
        let _df = read_json_local(uri, None, None, None)?;
        Ok(())
    }

    #[test]
    fn test_read_with_limit() -> DaftResult<()> {
        let uri = "/Users/corygrinstead/Downloads/stackexchange_sample.jsonl";
        let limit = 100;
        let convert_opts = JsonConvertOptions {
            limit: Some(limit),
            ..Default::default()
        };

        let df = read_json_local(uri, Some(convert_opts), None, None)?;
        assert_eq!(df.len(), limit);
        Ok(())
    }
}
