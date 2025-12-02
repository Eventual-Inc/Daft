use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::{prelude::*, utils::arrow::cast_array_for_daft_if_needed};
use daft_dsl::{Expr, ExprRef, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_recordbatch::RecordBatch;
use indexmap::IndexMap;
use num_traits::Pow;
use rayon::{ThreadPoolBuilder, prelude::*};
use serde_json::value::RawValue;
use snafu::ResultExt;

use crate::{
    ArrowSnafu, JsonConvertOptions, JsonParseOptions, JsonReadOptions, RayonThreadPoolSnafu,
    StdIOSnafu,
    decoding::{allocate_array, deserialize_into},
    deserializer::Value,
    inference::{column_types_map_to_fields, infer_records_schema},
    read::tables_concat,
};

const NEWLINE: u8 = b'\n';
const CLOSING_BRACKET: u8 = b'}';

#[inline]
fn can_pushdown(schema: &Schema, required_pred_cols: &HashSet<String>) -> bool {
    required_pred_cols
        .iter()
        .all(|c| match schema.get_field(c) {
            Ok(f) => f.dtype != DataType::Null,
            Err(_) => false,
        })
}

pub fn read_json_local(
    uri: &str,
    convert_options: Option<JsonConvertOptions>,
    parse_options: Option<JsonParseOptions>,
    read_options: Option<JsonReadOptions>,
    max_chunks_in_flight: Option<usize>,
) -> DaftResult<RecordBatch> {
    let uri = uri.trim_start_matches("file://");
    let file = std::fs::File::open(uri)?;
    // SAFETY: mmapping is inherently unsafe.
    // We are trusting that the file is not modified or accessed by other systems while we are reading it.
    let mmap = unsafe { memmap2::Mmap::map(&file) }.context(StdIOSnafu)?;

    let bytes = &mmap[..];
    if bytes.is_empty() {
        return Err(super::Error::JsonDeserializationError {
            string: "Invalid JSON format - file is empty".to_string(),
        }
        .into());
    }
    if bytes[0] == b'[' {
        // Prefer provided schema; otherwise infer once and reuse
        let mut decode_schema: Schema = match convert_options
            .as_ref()
            .and_then(|options| options.schema.as_ref())
        {
            Some(schema) => schema.as_ref().clone(),
            None => infer_schema(bytes, None, None)?.into(),
        };
        // Reuse inferred schema if no provided schema
        let inferred_for_hints: Option<Schema> = if convert_options
            .as_ref()
            .and_then(|options| options.schema.as_ref())
            .is_none()
        {
            Some(decode_schema.clone())
        } else {
            None
        };

        let predicate = convert_options
            .as_ref()
            .and_then(|options| options.predicate.clone());

        // Build hints once (use owned String keys to avoid lifetimes issues)
        let mut hinted_dtypes: HashMap<String, Field> = HashMap::new();
        if let Some(schema_hint) = convert_options.as_ref().and_then(|co| co.schema.as_ref()) {
            for field in schema_hint.fields() {
                hinted_dtypes.insert(field.name.clone(), field.clone());
            }
        }
        if let Some(local_inferred) = inferred_for_hints.as_ref() {
            for field in local_inferred.fields() {
                hinted_dtypes
                    .entry(field.name.clone())
                    .or_insert_with(|| field.clone());
            }
        } else {
            // Only infer if we need hints for missing columns not in the provided schema
            let existing: HashSet<&str> = decode_schema
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect();
            let mut needs_infer = convert_options
                .as_ref()
                .and_then(|co| co.include_columns.as_ref())
                .is_some_and(|cols| cols.iter().any(|c| !existing.contains(c.as_str())));
            if let Some(pred) = &predicate {
                for rc in get_required_columns(pred) {
                    if !existing.contains(rc.as_str()) {
                        needs_infer = true;
                        break;
                    }
                }
            }
            if needs_infer {
                let local_inferred: Schema = infer_schema(bytes, None, None)?.into();
                for field in local_inferred.fields() {
                    hinted_dtypes
                        .entry(field.name.clone())
                        .or_insert_with(|| field.clone());
                }
            }
        }

        let output_columns: Option<Vec<String>> = convert_options
            .as_ref()
            .and_then(|co| co.include_columns.clone());

        let mut required_pred_cols: HashSet<String> = HashSet::new();
        if let Some(pred) = &predicate {
            for rc in get_required_columns(pred) {
                required_pred_cols.insert(rc);
            }
        }

        let existing: HashSet<&str> = decode_schema
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let mut fields = decode_schema.fields().to_vec();
        if let Some(ref include_columns) = output_columns {
            for col in include_columns {
                if !existing.contains(col.as_str()) {
                    if let Some(hinted) = hinted_dtypes.get(col) {
                        fields.push(hinted.clone());
                    } else {
                        fields.push(Field::new(col.as_str(), DataType::Null));
                    }
                }
            }
        }
        for col in &required_pred_cols {
            if !existing.contains(col.as_str()) {
                if let Some(hinted) = hinted_dtypes.get(col) {
                    fields.push(hinted.clone());
                } else {
                    fields.push(Field::new(col.as_str(), DataType::Null));
                }
            }
        }
        decode_schema = Schema::new(fields);

        let can_pd = can_pushdown(&decode_schema, &required_pred_cols);
        let mut batch = read_json_array_impl(
            bytes,
            decode_schema.clone(),
            if can_pd { predicate.clone() } else { None },
        )?;
        if let Some(pred) = &predicate
            && !can_pd
        {
            let bound = BoundExpr::try_new(pred.clone(), &decode_schema)?;
            batch = batch.filter(&[bound])?;
        }
        if let Some(include_columns) = output_columns {
            let projected_schema = batch.schema.clone().project(&include_columns)?;
            let column_indices = include_columns
                .iter()
                .map(|col| batch.schema.get_index(col))
                .collect::<DaftResult<Vec<_>>>()?;
            batch = batch.get_columns(&column_indices);
            batch.schema = projected_schema.into();
        }
        Ok(batch)
    } else {
        let reader = JsonReader::try_new(
            bytes,
            convert_options,
            parse_options,
            read_options,
            max_chunks_in_flight,
        )?;
        reader.finish()
    }
}

pub fn read_json_array_impl(
    bytes: &[u8],
    schema: Schema,
    predicate: Option<ExprRef>,
) -> DaftResult<RecordBatch> {
    let mut scratch = vec![];
    let scratch = &mut scratch;

    let daft_fields = schema.into_iter().cloned().map(Arc::new);

    let arrow_schema = schema.to_arrow()?;

    let iter =
        serde_json::Deserializer::from_slice(bytes).into_iter::<&serde_json::value::RawValue>();

    let mut columns = arrow_schema
        .fields
        .iter()
        .map(|f| (Cow::Owned(f.name.clone()), allocate_array(f, bytes.len())))
        .collect::<IndexMap<_, _>>();

    let mut num_rows = 0;
    for record in iter {
        let value = record.map_err(|e| super::Error::JsonDeserializationError {
            string: e.to_string(),
        })?;
        let v = parse_raw_value(value, scratch)?;

        match v {
            Value::Array(arr) => {
                for value in arr {
                    if let Value::Object(record) = value {
                        for (s, inner) in &mut columns {
                            match record.get(s) {
                                Some(value) => {
                                    deserialize_into(inner, &[value]);
                                }
                                None => {
                                    inner.push_null();
                                }
                            }
                        }
                    } else {
                        Err(super::Error::JsonDeserializationError {
                            string: "Expected JSON object".to_string(),
                        })?;
                    }
                    num_rows += 1;
                }
            }
            Value::Object(record) => {
                for (s, inner) in &mut columns {
                    match record.get(s) {
                        Some(value) => {
                            deserialize_into(inner, &[value]);
                        }
                        None => {
                            inner.push_null();
                        }
                    }
                }
                num_rows += 1;
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
        .zip(daft_fields)
        .map(|(mut ma, fld)| {
            let arr = ma.as_box();
            Series::try_from_field_and_arrow_array(fld, cast_array_for_daft_if_needed(arr))
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let tbl = RecordBatch::new_unchecked(schema.clone(), columns, num_rows);

    if let Some(pred) = &predicate {
        let pred = BoundExpr::try_new(pred.clone(), &schema)?;
        tbl.filter(&[pred])
    } else {
        Ok(tbl)
    }
}

struct JsonReader<'a> {
    bytes: &'a [u8],
    decode_schema: SchemaRef,
    output_columns: Option<Vec<String>>,
    n_threads: usize,
    predicate: Option<Arc<Expr>>,
    can_pushdown: bool,
    sample_size: usize,
    n_rows: Option<usize>,
    chunk_size: Option<usize>,
    pool: rayon::ThreadPool,
}

impl<'a> JsonReader<'a> {
    fn try_new(
        bytes: &'a [u8],
        convert_options: Option<JsonConvertOptions>,
        parse_options: Option<JsonParseOptions>,
        read_options: Option<JsonReadOptions>,
        max_chunks_in_flight: Option<usize>,
    ) -> DaftResult<Self> {
        let sample_size = parse_options
            .as_ref()
            .and_then(|options| options.sample_size)
            .unwrap_or(1024);
        let n_rows = convert_options.as_ref().and_then(|options| options.limit);
        let chunk_size = read_options.as_ref().and_then(|options| options.chunk_size);
        let predicate = convert_options
            .as_ref()
            .and_then(|options| options.predicate.clone());

        let n_threads: usize = std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .into();

        let base_schema: SchemaRef = match convert_options
            .as_ref()
            .and_then(|options| options.schema.as_ref())
        {
            Some(schema) => schema.clone(),
            None => Arc::new(infer_schema(bytes, None, None)?.into()),
        };

        let pool = if let Some(max_in_flight) = max_chunks_in_flight {
            ThreadPoolBuilder::new().num_threads(max_in_flight).build()
        } else {
            ThreadPoolBuilder::new().num_threads(n_threads).build()
        }
        .context(RayonThreadPoolSnafu)?;

        let output_columns = convert_options
            .as_ref()
            .and_then(|options| options.include_columns.clone());

        let mut hinted_dtypes: HashMap<String, Field> = HashMap::new();
        if let Some(schema_hint) = convert_options.as_ref().and_then(|co| co.schema.as_ref()) {
            for field in schema_hint.fields() {
                hinted_dtypes.insert(field.name.clone(), field.clone());
            }
        }
        // Reuse base_schema as hints if it was inferred; else infer once for hints only if needed
        if convert_options
            .as_ref()
            .and_then(|options| options.schema.as_ref())
            .is_none()
        {
            for field in base_schema.fields() {
                hinted_dtypes
                    .entry(field.name.clone())
                    .or_insert_with(|| field.clone());
            }
        } else {
            let existing: HashSet<&str> = base_schema
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect();
            let mut needs_infer = convert_options
                .as_ref()
                .and_then(|co| co.include_columns.as_ref())
                .is_some_and(|cols| cols.iter().any(|c| !existing.contains(c.as_str())));
            if let Some(pred) = &predicate {
                for rc in get_required_columns(pred) {
                    if !existing.contains(rc.as_str()) {
                        needs_infer = true;
                        break;
                    }
                }
            }
            if needs_infer {
                let local_inferred: Schema = infer_schema(bytes, None, None)?.into();
                for field in local_inferred.fields() {
                    hinted_dtypes
                        .entry(field.name.clone())
                        .or_insert_with(|| field.clone());
                }
            }
        }

        let mut required_pred_cols: HashSet<String> = HashSet::new();
        if let Some(pred) = &predicate {
            for rc in get_required_columns(pred) {
                required_pred_cols.insert(rc);
            }
        }

        let existing: HashSet<&str> = base_schema
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let mut fields = base_schema.fields().to_vec();
        if let Some(ref include_columns) = output_columns {
            for col in include_columns {
                if !existing.contains(col.as_str()) {
                    if let Some(hinted) = hinted_dtypes.get(col) {
                        fields.push(hinted.clone());
                    } else {
                        fields.push(Field::new(col.as_str(), DataType::Null));
                    }
                }
            }
        }
        for col in &required_pred_cols {
            if !existing.contains(col.as_str()) {
                if let Some(hinted) = hinted_dtypes.get(col) {
                    fields.push(hinted.clone());
                } else {
                    fields.push(Field::new(col.as_str(), DataType::Null));
                }
            }
        }
        let decode_schema = Arc::new(Schema::new(fields));

        let can_pushdown_flag = can_pushdown(&decode_schema, &required_pred_cols);

        Ok(Self {
            bytes,
            decode_schema,
            output_columns,
            predicate,
            can_pushdown: can_pushdown_flag,
            n_threads,
            sample_size,
            n_rows,
            chunk_size,
            pool,
        })
    }

    pub fn finish(&self) -> DaftResult<RecordBatch> {
        let mut bytes = self.bytes;
        let mut n_threads = self.n_threads;
        let mut total_rows = 128;

        if let Some((mean, std)) = get_line_stats_json(bytes, self.sample_size) {
            let line_length_upper_bound = 1.1f32.mul_add(std, mean);

            total_rows = (bytes.len() as f32 / 0.01f32.mul_add(-std, mean)) as usize;
            if let Some(n_rows) = self.n_rows {
                total_rows = std::cmp::min(n_rows, total_rows);
                let n_bytes = (line_length_upper_bound * (n_rows as f32)) as usize;

                if n_bytes < bytes.len()
                    && let Some(pos) = next_line_position(&bytes[n_bytes..])
                {
                    bytes = &bytes[..n_bytes + pos];
                }
            }
        }

        if total_rows <= 128 {
            n_threads = 1;
        }

        let total_len = bytes.len();
        let chunk_size = self.chunk_size.unwrap_or_else(|| total_len / n_threads);
        let file_chunks = self.get_file_chunks(bytes, n_threads, total_len, chunk_size);

        let tbls = self.pool.install(|| {
            file_chunks
                .into_par_iter()
                .map(|(start, stop)| {
                    let chunk = &bytes[start..stop];
                    self.parse_json_chunk(chunk, chunk_size)
                })
                .collect::<DaftResult<Vec<RecordBatch>>>()
        })?;

        let mut tbl = tables_concat(tbls)?;

        if let Some(limit) = self.n_rows
            && tbl.len() > limit
        {
            tbl = tbl.head(limit)?;
        }

        if let Some(pred) = &self.predicate
            && self.can_pushdown
        {
            let bound = BoundExpr::try_new(pred.clone(), &self.decode_schema)?;
            tbl = tbl.filter(&[bound])?;
        }

        if let Some(include_columns) = &self.output_columns {
            let projected_schema = tbl.schema.clone().project(include_columns)?;
            let column_indices = include_columns
                .iter()
                .map(|col| tbl.schema.get_index(col))
                .collect::<DaftResult<Vec<_>>>()?;
            tbl = tbl.get_columns(&column_indices);
            tbl.schema = projected_schema.into();
        }

        Ok(tbl)
    }

    fn parse_json_chunk(&self, bytes: &[u8], chunk_size: usize) -> DaftResult<RecordBatch> {
        let mut scratch = vec![];
        let scratch = &mut scratch;

        let daft_fields = self.decode_schema.into_iter().cloned().map(Arc::new);

        let arrow_schema = self.decode_schema.to_arrow()?;

        let iter =
            serde_json::Deserializer::from_slice(bytes).into_iter::<&serde_json::value::RawValue>();

        let mut columns = arrow_schema
            .fields
            .iter()
            .map(|f| (Cow::Owned(f.name.clone()), allocate_array(f, chunk_size)))
            .collect::<IndexMap<_, _>>();

        let mut num_rows = 0;
        for record in iter {
            let value = record.map_err(|e| super::Error::JsonDeserializationError {
                string: e.to_string(),
            })?;
            let v = parse_raw_value(value, scratch)?;

            match v {
                Value::Object(record) => {
                    for (s, inner) in &mut columns {
                        match record.get(s) {
                            Some(value) => {
                                deserialize_into(inner, &[value]);
                            }
                            None => {
                                inner.push_null();
                            }
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

            num_rows += 1;
        }
        let columns = columns
            .into_values()
            .zip(daft_fields)
            .map(|(mut ma, fld)| {
                let arr = ma.as_box();
                Series::try_from_field_and_arrow_array(fld, cast_array_for_daft_if_needed(arr))
            })
            .collect::<DaftResult<Vec<_>>>()?;

        RecordBatch::new_with_size(self.decode_schema.clone(), columns, num_rows)
    }

    fn get_file_chunks(
        &self,
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
}

fn infer_schema(
    bytes: &[u8],
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
) -> DaftResult<arrow2::datatypes::Schema> {
    let max_bytes = max_bytes.unwrap_or(1024 * 1024);
    let max_records = max_rows.unwrap_or(1024);

    let mut total_bytes = 0;

    let mut column_types: IndexMap<String, HashSet<arrow2::datatypes::DataType>> = IndexMap::new();
    let mut scratch = Vec::new();
    let scratch = &mut scratch;

    let iter =
        serde_json::Deserializer::from_slice(bytes).into_iter::<&serde_json::value::RawValue>();

    for value in iter.take(max_records) {
        let value = value?;
        let bytes = value.get().as_bytes();
        total_bytes += bytes.len();

        let v = parse_raw_value(value, scratch)?;

        let inferred_schema = infer_records_schema(&v).context(ArrowSnafu)?;
        for field in inferred_schema.fields {
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

fn get_line_stats_json(bytes: &[u8], n_lines: usize) -> Option<(f32, f32)> {
    let mut lengths = Vec::with_capacity(n_lines);

    let mut bytes_trunc;
    let n_lines_per_iter = n_lines / 2;

    let mut n_read = 0;

    let bytes_len = bytes.len();

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
    for &len in &lengths {
        std += (len as f32 - mean).pow(2.0);
    }
    std = (std / n_samples as f32).sqrt();
    Some((mean, std))
}

fn calculate_chunks_and_size(n_threads: usize, chunk_size: usize, total: usize) -> (usize, usize) {
    let mut max_divisible_chunks = n_threads;
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

#[inline(always)]
fn parse_raw_value<'a>(
    raw_value: &'a RawValue,
    scratch: &'a mut Vec<u8>,
) -> crate::Result<Value<'a>> {
    let bytes = raw_value.get().as_bytes();
    scratch.clear();
    scratch.extend_from_slice(bytes);
    crate::deserializer::to_value(scratch).map_err(|e| super::Error::JsonDeserializationError {
        string: e.to_string(),
    })
}

fn next_line_position(input: &[u8]) -> Option<usize> {
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
    use arrow2::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_infer_schema() {
        let json = r#"
{"floats": 1.0, "utf8": "hello", "bools": true}
{"floats": 2.0, "utf8": "world", "bools": false}
{"floats": 3.0, "utf8": "!\n", "bools": true}
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
        let json = r"";

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
        let reader = JsonReader::try_new(json.as_bytes(), None, None, None, None).unwrap();
        let _result = reader.finish();
    }

    #[test]
    fn test_array_local_pushdown_include_disjoint_predicate() -> DaftResult<()> {
        let json =
            r#"[{"x":1,"y":"a","z":true},{"x":2,"y":"b","z":false},{"x":3,"y":"c","z":true}]"#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let uri = format!("file://{}", tmp.path().to_string_lossy());

        let pred: ExprRef = daft_dsl::unresolved_col("x").eq(daft_dsl::lit(2));
        let co = Some(
            JsonConvertOptions::default()
                .with_include_columns(Some(vec!["y".to_string()]))
                .with_predicate(Some(pred.clone())),
        );

        let out = read_json_local(&uri, co, None, None, None)?;
        assert_eq!(out.len(), 1);
        assert!(out.schema.get_index("y").is_ok());
        let y_idx = out.schema.get_index("y")?;
        let col_y = out.get_column(y_idx);
        assert_eq!(
            col_y
                .to_arrow()
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap()
                .value(0),
            "b"
        );
        Ok(())
    }

    #[test]
    fn test_array_local_predicate_only_with_provided_schema_missing_pred_col() -> DaftResult<()> {
        let json = r#"[{"x":1,"y":"a"},{"x":2,"y":"b"},{"x":3,"y":"c"}]"#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let uri = format!("file://{}", tmp.path().to_string_lossy());

        let provided_schema = Schema::new(vec![Field::new("y", DataType::Utf8)]);
        let pred: ExprRef = daft_dsl::unresolved_col("x").eq(daft_dsl::lit(2));
        let co = Some(
            JsonConvertOptions::default()
                .with_schema(Some(provided_schema.into()))
                .with_predicate(Some(pred.clone())),
        );

        let out = read_json_local(&uri, co, None, None, None)?;
        assert_eq!(out.len(), 1);
        assert!(out.schema.get_index("y").is_ok());
        Ok(())
    }

    #[test]
    fn test_line_local_pushdown_include_disjoint_predicate_missing_pred_in_provided_schema()
    -> DaftResult<()> {
        let json = r#"
{"x":1,"y":"a"}
{"x":2,"y":"b"}
{"x":3,"y":"c"}
"#;
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let uri = format!("file://{}", tmp.path().to_string_lossy());

        let provided_schema = Schema::new(vec![Field::new("y", DataType::Utf8)]);
        let pred: ExprRef = daft_dsl::unresolved_col("x").eq(daft_dsl::lit(2));
        let co = Some(
            JsonConvertOptions::default()
                .with_schema(Some(provided_schema.into()))
                .with_include_columns(Some(vec!["y".to_string()]))
                .with_predicate(Some(pred.clone())),
        );

        let out = read_json_local(&uri, co, None, None, None)?;
        assert_eq!(out.len(), 1);
        assert!(out.schema.get_index("y").is_ok());
        let y_idx = out.schema.get_index("y")?;
        let col_y = out.get_column(y_idx);
        assert_eq!(
            col_y
                .to_arrow()
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap()
                .value(0),
            "b"
        );
        Ok(())
    }
}
