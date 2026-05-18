//! Pure-function helpers for the v2 reader: `RowSelection` arithmetic,
//! mask truncation, schema projection, and the `parquet_err` adapter.
//! No I/O, no decode.

use std::collections::HashSet;

use arrow::array::{Array, BooleanArray};
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_recordbatch::RecordBatch;
use parquet::arrow::arrow_reader::RowSelection;

/// Wrap any `Display` error (typically `parquet::errors::ParquetError` or
/// `arrow::error::ArrowError`) into a `DaftError::ValueError` tagged with a
/// "parquet decode:" prefix. Used throughout the v2 reader.
pub(super) fn parquet_err<E: std::fmt::Display>(e: E) -> common_error::DaftError {
    common_error::DaftError::ValueError(format!("parquet decode: {}", e))
}

/// Cap a `RowSelection` (or "select all rg_rows" when None) so the total
/// selected row count is exactly `cap`. Skipped rows after the cap remain
/// skipped.
pub(super) fn cap_selection_to(
    base: Option<&RowSelection>,
    cap: usize,
    rg_rows: usize,
) -> RowSelection {
    let selectors: Vec<parquet::arrow::arrow_reader::RowSelector> = match base {
        Some(s) => s.iter().copied().collect(),
        None => vec![parquet::arrow::arrow_reader::RowSelector::select(rg_rows)],
    };
    let mut out = Vec::with_capacity(selectors.len() + 1);
    let mut taken = 0usize;
    for sel in selectors {
        if sel.skip {
            out.push(sel);
            continue;
        }
        if taken >= cap {
            // Convert remaining selects to skips.
            out.push(parquet::arrow::arrow_reader::RowSelector::skip(
                sel.row_count,
            ));
            continue;
        }
        let want = cap - taken;
        if sel.row_count <= want {
            out.push(sel);
            taken += sel.row_count;
        } else {
            out.push(parquet::arrow::arrow_reader::RowSelector::select(want));
            out.push(parquet::arrow::arrow_reader::RowSelector::skip(
                sel.row_count - want,
            ));
            taken = cap;
        }
    }
    RowSelection::from(out)
}

/// Zero out the mask after the position of the nth true bit. Used to push a
/// limit through a predicate: if a RG produces more matches than we need,
/// we keep only the first N trues so phase-2 decode and assembly produce
/// exactly `n` rows from this RG.
pub(super) fn truncate_mask_to_n_trues(mask: &BooleanArray, n: usize) -> BooleanArray {
    let len = mask.len();
    let mut count = 0usize;
    let mut cutoff = len;
    for i in 0..len {
        if mask.is_valid(i) && mask.value(i) {
            if count == n {
                cutoff = i;
                break;
            }
            count += 1;
        }
    }
    if cutoff == len {
        return mask.clone();
    }
    let buf: Vec<bool> = (0..len)
        .map(|i| i < cutoff && mask.is_valid(i) && mask.value(i))
        .collect();
    BooleanArray::from(buf)
}

pub(super) fn project_schema(schema: &Schema, names: &HashSet<String>) -> Schema {
    let fields: Vec<Field> = schema
        .field_names()
        .filter(|n| names.contains(*n))
        .filter_map(|n| schema.get_field(n).ok().cloned())
        .collect();
    Schema::new(fields)
}

pub(super) fn project_to_schema(
    batch: RecordBatch,
    return_schema: &Schema,
) -> DaftResult<RecordBatch> {
    if batch.schema.len() == return_schema.len()
        && batch
            .schema
            .field_names()
            .zip(return_schema.field_names())
            .all(|(a, b)| a == b)
    {
        return Ok(batch);
    }
    let mut indices: Vec<usize> = Vec::with_capacity(return_schema.len());
    for name in return_schema.field_names() {
        let (idx, _) = batch
            .schema
            .get_fields_with_name(name)
            .into_iter()
            .next()
            .ok_or_else(|| {
                common_error::DaftError::ValueError(format!(
                    "project_to_schema: column '{}' not found",
                    name
                ))
            })?;
        indices.push(idx);
    }
    Ok(batch.get_columns(&indices))
}
