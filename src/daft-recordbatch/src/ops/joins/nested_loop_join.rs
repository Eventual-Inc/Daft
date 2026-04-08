use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;

use crate::{
    RecordBatch,
    column::{Column, ScalarColumn},
};

/// Perform an inner nested loop join between two record batches, evaluating the given
/// predicate on all pairs of rows and returning only rows where the predicate holds.
///
/// For each left row, left columns are represented as O(1) scalar columns broadcast to
/// `right.len()` rows. The predicate is evaluated on the combined batch and matching rows
/// are collected. Peak memory per iteration is O(right.len()) for the right-side columns.
pub(super) fn nested_loop_join(
    left: &RecordBatch,
    right: &RecordBatch,
    predicate: &[BoundExpr],
    join_schema: &SchemaRef,
) -> DaftResult<RecordBatch> {
    if left.is_empty() || right.is_empty() {
        return Ok(RecordBatch::empty(Some(join_schema.clone())));
    }

    // Precompute the mapping from each join_schema field to its source column index.
    // `true` means the column comes from the left batch; `false` means right.
    // Done once here so the inner loop avoids repeated hashmap lookups per row.
    let col_sources: Vec<(bool, usize)> = join_schema
        .as_ref()
        .into_iter()
        .map(|field| {
            if let Ok(idx) = left.schema.get_index(&field.name) {
                Ok((true, idx))
            } else {
                right.schema.get_index(&field.name).map(|idx| (false, idx))
            }
        })
        .collect::<DaftResult<_>>()?;

    let mut results: Vec<RecordBatch> = Vec::new();
    for i in 0..left.len() {
        let row_result =
            match_left_row_against_right(left, right, i, predicate, join_schema, &col_sources)?;
        if !row_result.is_empty() {
            results.push(row_result);
        }
    }

    if results.is_empty() {
        Ok(RecordBatch::empty(Some(join_schema.clone())))
    } else {
        RecordBatch::concat(results.as_slice())
    }
}

/// For a single left row `i`, evaluate the predicate against all right rows and return the
/// matching rows from the combined (left scalar + right series) batch.
///
/// Left columns are O(1) `ScalarColumn`s broadcast to `right.len()` rows so the full
/// cartesian product is never materialised — only the rows that pass the filter.
fn match_left_row_against_right(
    left: &RecordBatch,
    right: &RecordBatch,
    i: usize,
    predicate: &[BoundExpr],
    join_schema: &SchemaRef,
    col_sources: &[(bool, usize)],
) -> DaftResult<RecordBatch> {
    let m = right.len();
    let mut combined = Vec::with_capacity(col_sources.len());

    for &(is_left, idx) in col_sources {
        if is_left {
            let s = left.get_column(idx);
            combined.push(Column::Scalar(ScalarColumn::new(
                Arc::from(s.name()),
                s.data_type().clone(),
                s.get_lit(i),
                m,
            )));
        } else {
            combined.push(Column::Series(right.get_column(idx).clone()));
        }
    }

    let batch = RecordBatch::new_unchecked_with_columns(join_schema.clone(), combined, m);
    batch.filter(predicate)
}
