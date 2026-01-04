use std::{cmp, iter::repeat_n, ops::Not, sync::Arc};

use arrow_array::builder::BooleanBufferBuilder;
use common_error::DaftResult;
use daft_arrow::{buffer::NullBufferBuilder, types::IndexRange};
use daft_core::{
    array::ops::{DaftIsNull, arrow::comparison::build_multi_array_is_equal},
    prelude::*,
};
use daft_dsl::{
    expr::bound_expr::BoundExpr,
    join::{get_common_join_cols, infer_join_schema},
};

use super::{
    add_non_join_key_columns, get_column_by_name, get_columns_by_name, match_types_for_tables,
};
use crate::RecordBatch;
pub(super) fn hash_inner_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_on: &[BoundExpr],
    right_on: &[BoundExpr],
    null_equals_nulls: &[bool],
) -> DaftResult<RecordBatch> {
    let join_schema = infer_join_schema(&left.schema, &right.schema, JoinType::Inner)?;
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    let (lidx, ridx) = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        (
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
        )
    } else {
        // probe on the smaller table
        let probe_left = lkeys.len() <= rkeys.len();

        let (lkeys, rkeys) = if probe_left {
            (&lkeys, &rkeys)
        } else {
            (&rkeys, &lkeys)
        };

        let probe_table = lkeys.to_probe_hash_table()?;

        let r_hashes = rkeys.hash_rows()?;
        use daft_core::array::ops::arrow::comparison::build_multi_array_is_equal;
        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            null_equals_nulls,
            vec![false; lkeys.columns.len()].as_slice(),
        )?;

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (r_idx, h) in r_hashes.values().iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    right_idx.push(r_idx as u64);
                }
            }
        }

        let larr = UInt64Array::from(("left_indices", left_idx));
        let rarr = UInt64Array::from(("right_indices", right_idx));

        if probe_left {
            (larr, rarr)
        } else {
            (rarr, larr)
        }
    };

    let common_cols: Vec<_> = get_common_join_cols(&left.schema, &right.schema).collect();

    let mut join_series = Arc::unwrap_or_clone(
        get_columns_by_name(left, &common_cols)?
            .take(&lidx)?
            .columns,
    );

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series = add_non_join_key_columns(left, right, lidx, ridx, join_series)?;

    RecordBatch::new_with_size(join_schema, join_series, num_rows)
}

pub(super) fn hash_left_right_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_on: &[BoundExpr],
    right_on: &[BoundExpr],
    null_equals_nulls: &[bool],
    left_side: bool,
) -> DaftResult<RecordBatch> {
    let join_schema = infer_join_schema(&left.schema, &right.schema, JoinType::Right)?;
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    // For left joins, swap tables and use right join, since we want to probe the side we are joining from
    let (lkeys, rkeys) = if left_side {
        (rkeys, lkeys)
    } else {
        (lkeys, rkeys)
    };

    let (lidx, ridx) = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        (
            UInt64Array::full_null("left_indices", &DataType::UInt64, rkeys.len()),
            UInt64Array::from((
                "right_indices",
                (0..(rkeys.len() as u64)).collect::<Vec<_>>(),
            )),
        )
    } else {
        let probe_table = lkeys.to_probe_hash_table()?;

        let r_hashes = rkeys.hash_rows()?;

        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            null_equals_nulls,
            vec![false; lkeys.columns.len()].as_slice(),
        )?;

        // we will have at least as many rows in the join table as the right table
        let min_rows = rkeys.len();

        let mut left_idx = Vec::with_capacity(min_rows);
        let mut right_idx = Vec::with_capacity(min_rows);

        let mut l_valid = NullBufferBuilder::new(min_rows);

        for (r_idx, h) in r_hashes.values().iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    right_idx.push(r_idx as u64);
                    l_valid.append_non_null();
                }
            } else {
                left_idx.push(0);
                right_idx.push(r_idx as u64);
                l_valid.append_null();
            }
        }

        (
            UInt64Array::from(("left_indices", left_idx)).with_validity(l_valid.finish())?,
            UInt64Array::from(("right_indices", right_idx)),
        )
    };

    // swap back the tables
    let (lkeys, rkeys, lidx, ridx) = if left_side {
        (rkeys, lkeys, ridx, lidx)
    } else {
        (lkeys, rkeys, lidx, ridx)
    };

    let common_cols = get_common_join_cols(&left.schema, &right.schema).collect::<Vec<_>>();

    let (common_cols_tbl, common_cols_idx) = if left_side {
        (left, &lidx)
    } else {
        (right, &ridx)
    };

    let mut join_series = Arc::unwrap_or_clone(
        get_columns_by_name(common_cols_tbl, &common_cols)?
            .take(common_cols_idx)?
            .columns,
    );

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series = add_non_join_key_columns(left, right, lidx, ridx, join_series)?;

    RecordBatch::new_with_size(join_schema, join_series, num_rows)
}

pub(super) fn hash_semi_anti_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_on: &[BoundExpr],
    right_on: &[BoundExpr],
    null_equals_nulls: &[bool],
    is_anti: bool,
) -> DaftResult<RecordBatch> {
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    let lidx = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        if is_anti {
            // if we have a null column match, then all of the rows match for an anti join!
            return Ok(left.clone());
        } else {
            UInt64Array::empty("left_indices", &DataType::UInt64)
        }
    } else {
        let probe_table = rkeys.to_probe_hash_map_without_idx()?;

        let l_hashes = lkeys.hash_rows()?;

        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            null_equals_nulls,
            vec![false; lkeys.columns.len()].as_slice(),
        )?;
        let rows = rkeys.len();

        drop(lkeys);
        drop(rkeys);

        let mut left_idx = Vec::with_capacity(rows);
        let is_semi = !is_anti;
        for (l_idx, h) in l_hashes.values().iter().enumerate() {
            let is_match = probe_table
                .raw_entry()
                .from_hash(*h, |other| {
                    *h == other.hash && {
                        let r_idx = other.idx as usize;
                        is_equal(l_idx, r_idx)
                    }
                })
                .is_some();
            if is_match == is_semi {
                left_idx.push(l_idx as u64);
            }
        }

        UInt64Array::from(("left_indices", left_idx))
    };

    left.take(&lidx)
}

pub(super) fn hash_outer_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_on: &[BoundExpr],
    right_on: &[BoundExpr],
    null_equals_nulls: &[bool],
) -> DaftResult<RecordBatch> {
    let join_schema = infer_join_schema(&left.schema, &right.schema, JoinType::Outer)?;
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    let (lidx, ridx) = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        let l_iter = IndexRange::new(0, lkeys.len() as u64)
            .map(Some)
            .chain(repeat_n(None, rkeys.len()));
        let r_iter =
            repeat_n(None, lkeys.len()).chain(IndexRange::new(0, rkeys.len() as u64).map(Some));

        let l_arrow =
            Box::new(daft_arrow::array::PrimitiveArray::<u64>::from_trusted_len_iter(l_iter));
        let r_arrow =
            Box::new(daft_arrow::array::PrimitiveArray::<u64>::from_trusted_len_iter(r_iter));

        (
            UInt64Array::from(("left_indices", l_arrow)),
            UInt64Array::from(("right_indices", r_arrow)),
        )
    } else {
        // probe on the smaller table
        let probe_left = lkeys.len() <= rkeys.len();

        let (lkeys, rkeys) = if probe_left {
            (&lkeys, &rkeys)
        } else {
            (&rkeys, &lkeys)
        };

        let probe_table = lkeys.to_probe_hash_table()?;

        let r_hashes = rkeys.hash_rows()?;

        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            null_equals_nulls,
            vec![false; lkeys.columns.len()].as_slice(),
        )?;

        // we will have at least as many rows in the join table as the max of the left and right tables
        let min_rows = cmp::max(lkeys.len(), rkeys.len());

        let mut left_idx = Vec::with_capacity(min_rows);
        let mut right_idx = Vec::with_capacity(min_rows);

        let mut l_valid = NullBufferBuilder::new(min_rows);
        let mut r_valid = NullBufferBuilder::new(min_rows);

        let mut left_idx_used = BooleanBufferBuilder::new(lkeys.len());

        for (r_idx, h) in r_hashes.values().iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    left_idx_used.set_bit(*l_idx as usize, true);

                    right_idx.push(r_idx as u64);

                    l_valid.append_non_null();
                    r_valid.append_non_null();
                }
            } else {
                left_idx.push(0);
                right_idx.push(r_idx as u64);

                l_valid.append_null();
                r_valid.append_non_null();
            }
        }

        for (l_idx, used) in left_idx_used.finish().into_iter().enumerate() {
            if !used {
                left_idx.push(l_idx as u64);
                right_idx.push(0);

                l_valid.append_non_null();
                r_valid.append_null();
            }
        }

        let larr = UInt64Array::from(("left_indices", left_idx)).with_validity(l_valid.finish())?;
        let rarr =
            UInt64Array::from(("right_indices", right_idx)).with_validity(r_valid.finish())?;

        if probe_left {
            (larr, rarr)
        } else {
            (rarr, larr)
        }
    };

    let common_cols: Vec<_> = get_common_join_cols(&left.schema, &right.schema).collect();

    let mut join_series = if common_cols.is_empty() {
        vec![]
    } else {
        // use right side value if left is null
        let take_from_left = lidx.is_null()?.not()?.into_series();

        common_cols
            .into_iter()
            .map(|name| {
                let lcol = get_column_by_name(left, name)?.take(&lidx)?;
                let rcol = get_column_by_name(right, name)?.take(&ridx)?;

                lcol.if_else(&rcol, &take_from_left)
            })
            .collect::<DaftResult<Vec<_>>>()?
    };

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series = add_non_join_key_columns(left, right, lidx, ridx, join_series)?;

    RecordBatch::new_with_size(join_schema, join_series, num_rows)
}
