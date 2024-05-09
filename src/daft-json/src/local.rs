use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_core::{schema::Schema, utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_table::Table;
use indexmap::IndexMap;
use num_traits::Pow;

use rayon::prelude::*;
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
    let sample_size = parse_options
        .as_ref()
        .and_then(|options| options.sample_size)
        .unwrap_or(1024);
    let n_rows = convert_options.as_ref().and_then(|options| options.limit);
    let chunk_size = read_options.as_ref().and_then(|options| options.chunk_size);
    let file = std::fs::File::open(uri)?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }.expect("TODO");
    let bytes = &mmap[..];
    let schema = match convert_options.and_then(|options| options.schema) {
        Some(schema) => schema.as_ref().clone(),
        None => {
            let mut cursor = std::io::Cursor::new(bytes);
            Schema::try_from(&infer_schema(&mut cursor, None, None)?)?
        }
    };

    let n_threads = 8; // TODO: make this configurable
    read_json_impl(bytes, sample_size, n_rows, n_threads, chunk_size, schema)
}

fn read_json_impl(
    bytes: &[u8],
    sample_size: usize,
    n_rows: Option<usize>,
    mut n_threads: usize,
    chunk_size: Option<usize>,
    schema: Schema,
) -> DaftResult<Table> {
    let mut bytes = bytes;
    let mut total_rows = 128;

    if let Some((mean, std)) = get_line_stats_json(bytes, sample_size) {
        let line_length_upper_bound = mean + 1.1 * std;

        total_rows = (bytes.len() as f32 / (mean - 0.01 * std)) as usize;
        if let Some(n_rows) = n_rows {
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
    let chunk_size = chunk_size.unwrap_or_else(|| total_rows / n_threads);

    let file_chunks = get_file_chunks(bytes, n_threads, total_len, chunk_size);

    let tbls = file_chunks
        .into_par_iter()
        .map(|(start, stop)| {
            let chunk = &bytes[start..stop];
            parse_json_chunk(chunk, &schema, chunk_size)
        })
        .collect::<DaftResult<Vec<Table>>>()?;

    tables_concat(tbls)
}

fn infer_schema<R>(
    reader: &mut R,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
) -> DaftResult<arrow2::datatypes::Schema>
where
    R: std::io::BufRead,
{
    let max_records = max_rows.unwrap_or(usize::MAX);
    let max_bytes = max_bytes.unwrap_or(usize::MAX);
    let mut total_bytes = 0;
    let mut total_records = 0;

    let mut column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>> = IndexMap::new();
    let mut scratch = String::new();

    loop {
        let bytes_read = reader.read_line(&mut scratch).expect("TODO");
        if total_records == 0 && bytes_read > 0 && scratch.as_bytes() == [NEWLINE] {
            total_records += 1;
            total_bytes += bytes_read;
            scratch.clear();
            continue;
        }
        if bytes_read == 0 {
            break;
        }
        total_bytes += bytes_read;
        if total_bytes > max_bytes {
            break;
        }

        total_records += 1;
        if total_records > max_records {
            break;
        }
        if scratch.trim().is_empty() {
            break;
        }
        let parsed_record = crate::deserializer::to_value(unsafe { scratch.as_bytes_mut() })
            .map_err(|e| super::Error::JsonDeserializationError {
                string: e.to_string(),
            })?;

        let inferred_schema = infer_records_schema(&parsed_record).context(ArrowSnafu)?;
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
    }
    let fields = column_types_map_to_fields(column_types);
    Ok(fields.into())
}

/// Get the mean and standard deviation of length of lines in bytes
pub(crate) fn get_line_stats_json(bytes: &[u8], n_lines: usize) -> Option<(f32, f32)> {
    let mut lengths = Vec::with_capacity(n_lines);

    let mut bytes_trunc;
    let n_lines_per_iter = n_lines / 4;

    let mut n_read = 0;

    let bytes_len = bytes.len();

    // sample from 0, 25% 50% and 75% in the file
    for offset in [
        0,
        (bytes_len as f32 * 0.25) as usize,
        (bytes_len as f32 * 0.50) as usize,
        (bytes_len as f32 * 0.75) as usize,
    ] {
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

fn get_file_chunks(
    bytes: &[u8],
    n_threads: usize,
    total_len: usize,
    chunk_size: usize,
) -> Vec<(usize, usize)> {
    let mut last_pos = 0;

    let mut offsets = Vec::with_capacity(n_threads);
    for _ in 0..n_threads {
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
    // It is used to properly iterate over the lines without re-implementing the splitlines logic when this does the same thing.
    // This is faster and more accurate than using a `Lines` iterator.
    let iter =
        serde_json::Deserializer::from_slice(bytes).into_iter::<Box<serde_json::value::RawValue>>();

    let mut results = arrow_schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), allocate_array(f, chunk_size)))
        .collect::<IndexMap<_, _>>();

    for record in iter {
        let value = record.map_err(|e| super::Error::JsonDeserializationError {
            string: e.to_string(),
        })?;

        let bytes = value.get().as_bytes();

        scratch.clear();
        scratch.extend_from_slice(bytes);

        let v = crate::deserializer::to_value(scratch).map_err(|e| {
            super::Error::JsonDeserializationError {
                string: e.to_string(),
            }
        })?;

        match v {
            Value::Object(record) => {
                for (key, value) in record.iter() {
                    let arr = results.get_mut(key.as_ref());
                    if let Some(arr) = arr {
                        deserialize_into(arr, &[value]);
                    } else {
                        panic!("Field not found in schema");
                    }
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
    let columns = results
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
        let mut reader = std::io::BufReader::new(json.as_bytes());

        let result = infer_schema(&mut reader, None, None);
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
        let mut reader = std::io::BufReader::new(json.as_bytes());

        let result = infer_schema(&mut reader, None, None);
        let expected_schema = ArrowSchema::from(vec![]);
        assert_eq!(result.unwrap(), expected_schema);
    }

    #[test]
    fn test_read_json_impl() {
        let json = r#"
{"floats": 1.0, "utf8": "hello", "bools": true}
{"floats": 2.0, "utf8": "world", "bools": false}
{"floats": 3.0, "utf8": "!\\n", "bools": true}
"#;
        let schema = Schema::try_from(&ArrowSchema::from(vec![
            ArrowField::new("floats", ArrowDataType::Float64, true),
            ArrowField::new("utf8", ArrowDataType::Utf8, true),
            ArrowField::new("bools", ArrowDataType::Boolean, true),
        ]))
        .unwrap();
        let result = read_json_impl(json.as_bytes(), 1024, None, 8, None, schema);
        println!("tbl = {}", result.unwrap());
    }

    #[test]
    fn test_read_json() -> DaftResult<()> {
        // let df = read_json_local(json_path, None, None, None)?;
        // println!("df = {}", df);
        Ok(())
    }
}
