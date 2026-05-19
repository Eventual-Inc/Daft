use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, BooleanArray},
    datatypes::{Field as ArrowField, Schema as ArrowSchema},
};
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_recordbatch::RecordBatch;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

/// Build an `Arc<ArrowSchema>` containing the fields at the given positions
/// of `arrow_schema`, in order. Shared by every place that constructs a
/// per-column-subset arrow schema (prefilter, predicate-only processor, etc).
pub(super) fn schema_from_indices(
    arrow_schema: &ArrowSchema,
    indices: &[usize],
) -> Arc<ArrowSchema> {
    let fields: Vec<Arc<ArrowField>> = indices
        .iter()
        .map(|&i| Arc::new(arrow_schema.field(i).clone()))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

pub(super) fn cap_selection_to(
    base: Option<&RowSelection>,
    cap: usize,
    rg_rows: usize,
) -> RowSelection {
    let selectors: Vec<RowSelector> = match base {
        Some(s) => s.iter().copied().collect(),
        None => vec![RowSelector::select(rg_rows)],
    };
    let mut out = Vec::with_capacity(selectors.len() + 1);
    let mut taken = 0usize;
    for sel in selectors {
        if sel.skip {
            out.push(sel);
            continue;
        }
        if taken >= cap {
            out.push(RowSelector::skip(sel.row_count));
            continue;
        }
        let want = cap - taken;
        if sel.row_count <= want {
            out.push(sel);
            taken += sel.row_count;
        } else {
            out.push(RowSelector::select(want));
            out.push(RowSelector::skip(sel.row_count - want));
            taken = cap;
        }
    }
    RowSelection::from(out)
}

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
    if batch.schema.field_names().eq(return_schema.field_names()) {
        return Ok(batch);
    }
    let indices = return_schema
        .field_names()
        .map(|name| {
            batch
                .schema
                .get_fields_with_name(name)
                .into_iter()
                .next()
                .map(|(idx, _)| idx)
                .ok_or_else(|| {
                    common_error::DaftError::ValueError(format!(
                        "project_to_schema: column '{}' not found",
                        name
                    ))
                })
        })
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(batch.get_columns(&indices))
}
