use std::{cmp, iter::repeat};

use arrow2::{bitmap::MutableBitmap, types::IndexRange};
use daft_core::{
    array::ops::{arrow2::comparison::build_multi_array_is_equal, full::FullNull},
    datatypes::{BooleanArray, UInt64Array},
    DataType, IntoSeries, JoinType,
};
use daft_dsl::ExprRef;

use crate::{infer_join_schema, Table};
use common_error::DaftResult;

use daft_core::array::ops::as_arrow::AsArrow;

use super::{add_non_join_key_columns, match_types_for_tables};

pub(super) fn hash_inner_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
) -> DaftResult<Table> {
    let join_schema = infer_join_schema(
        &left.schema,
        &right.schema,
        left_on,
        right_on,
        JoinType::Inner,
    )?;
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    let (lidx, ridx) = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        (
            UInt64Array::empty("left_indices", &DataType::UInt64).into_series(),
            UInt64Array::empty("right_indices", &DataType::UInt64).into_series(),
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
            false,
            false,
        )?;

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
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

        let lseries = UInt64Array::from(("left_indices", left_idx)).into_series();
        let rseries = UInt64Array::from(("right_indices", right_idx)).into_series();

        if probe_left {
            (lseries, rseries)
        } else {
            (rseries, lseries)
        }
    };

    let mut join_series = left
        .get_columns(lkeys.column_names().as_slice())?
        .take(&lidx)?
        .columns;

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new_with_size(join_schema, join_series, num_rows)
}

pub(super) fn hash_left_right_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
    left_side: bool,
) -> DaftResult<Table> {
    let join_schema = infer_join_schema(
        &left.schema,
        &right.schema,
        left_on,
        right_on,
        JoinType::Right,
    )?;
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
            UInt64Array::full_null("left_indices", &DataType::UInt64, rkeys.len()).into_series(),
            UInt64Array::from((
                "right_indices",
                (0..(rkeys.len() as u64)).collect::<Vec<_>>(),
            ))
            .into_series(),
        )
    } else {
        let probe_table = lkeys.to_probe_hash_table()?;

        let r_hashes = rkeys.hash_rows()?;

        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            false,
            false,
        )?;

        // we will have at least as many rows in the join table as the right table
        let min_rows = rkeys.len();

        let mut left_idx = Vec::with_capacity(min_rows);
        let mut right_idx = Vec::with_capacity(min_rows);

        let mut l_valid = MutableBitmap::with_capacity(min_rows);

        for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    right_idx.push(r_idx as u64);
                    l_valid.push(true);
                }
            } else {
                left_idx.push(0);
                right_idx.push(r_idx as u64);
                l_valid.push(false);
            }
        }

        (
            UInt64Array::from(("left_indices", left_idx))
                .with_validity(Some(l_valid.into()))?
                .into_series(),
            UInt64Array::from(("right_indices", right_idx)).into_series(),
        )
    };

    // swap back the tables
    let (lkeys, rkeys, lidx, ridx) = if left_side {
        (rkeys, lkeys, ridx, lidx)
    } else {
        (lkeys, rkeys, lidx, ridx)
    };

    let mut join_series = if left_side {
        left.get_columns(lkeys.column_names().as_slice())?
            .take(&lidx)?
            .columns
    } else {
        lkeys
            .column_names()
            .iter()
            .zip(rkeys.column_names().iter())
            .map(|(l, r)| {
                let join_col = if l == r {
                    let col_dtype = left.get_column(l)?.data_type();
                    right.get_column(r)?.take(&ridx)?.cast(col_dtype)?
                } else {
                    left.get_column(l)?.take(&lidx)?
                };

                Ok(join_col)
            })
            .collect::<DaftResult<Vec<_>>>()?
    };

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new_with_size(join_schema, join_series, num_rows)
}

pub(super) fn hash_semi_anti_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
    is_anti: bool,
) -> DaftResult<Table> {
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
            UInt64Array::empty("left_indices", &DataType::UInt64).into_series()
        }
    } else {
        let probe_table = rkeys.to_probe_hash_map_without_idx()?;

        let l_hashes = lkeys.hash_rows()?;

        let is_equal = build_multi_array_is_equal(
            lkeys.columns.as_slice(),
            rkeys.columns.as_slice(),
            false,
            false,
        )?;
        let rows = rkeys.len();

        drop(lkeys);
        drop(rkeys);

        let mut left_idx = Vec::with_capacity(rows);
        let is_semi = !is_anti;
        for (l_idx, h) in l_hashes.as_arrow().values_iter().enumerate() {
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

        UInt64Array::from(("left_indices", left_idx)).into_series()
    };

    left.take(&lidx)
}

pub(super) fn hash_outer_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
) -> DaftResult<Table> {
    let join_schema = infer_join_schema(
        &left.schema,
        &right.schema,
        left_on,
        right_on,
        JoinType::Outer,
    )?;
    let lkeys = left.eval_expression_list(left_on)?;
    let rkeys = right.eval_expression_list(right_on)?;

    let (lkeys, rkeys) = match_types_for_tables(&lkeys, &rkeys)?;

    let (lidx, ridx) = if lkeys.columns.iter().any(|s| s.data_type().is_null())
        || rkeys.columns.iter().any(|s| s.data_type().is_null())
    {
        let l_iter = IndexRange::new(0, lkeys.len() as u64)
            .map(Some)
            .chain(repeat(None).take(rkeys.len()));
        let r_iter = repeat(None)
            .take(lkeys.len())
            .chain(IndexRange::new(0, rkeys.len() as u64).map(Some));

        let l_arrow = Box::new(arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(
            l_iter,
        ));
        let r_arrow = Box::new(arrow2::array::PrimitiveArray::<u64>::from_trusted_len_iter(
            r_iter,
        ));

        (
            UInt64Array::from(("left_indices", l_arrow)).into_series(),
            UInt64Array::from(("right_indices", r_arrow)).into_series(),
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
            false,
            false,
        )?;

        // we will have at least as many rows in the join table as the max of the left and right tables
        let min_rows = cmp::max(lkeys.len(), rkeys.len());

        let mut left_idx = Vec::with_capacity(min_rows);
        let mut right_idx = Vec::with_capacity(min_rows);

        let mut l_valid = MutableBitmap::with_capacity(min_rows);
        let mut r_valid = MutableBitmap::with_capacity(min_rows);

        let mut left_idx_used = MutableBitmap::from_len_zeroed(lkeys.len());

        for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    left_idx_used.set(*l_idx as usize, true);

                    right_idx.push(r_idx as u64);

                    l_valid.push(true);
                    r_valid.push(true);
                }
            } else {
                left_idx.push(0);
                right_idx.push(r_idx as u64);

                l_valid.push(false);
                r_valid.push(true);
            }
        }

        for (l_idx, used) in left_idx_used.into_iter().enumerate() {
            if !used {
                left_idx.push(l_idx as u64);
                right_idx.push(0);

                l_valid.push(true);
                r_valid.push(false);
            }
        }

        let lseries = UInt64Array::from(("left_indices", left_idx))
            .with_validity(Some(l_valid.into()))?
            .into_series();
        let rseries = UInt64Array::from(("right_indices", right_idx))
            .with_validity(Some(r_valid.into()))?
            .into_series();

        if probe_left {
            (lseries, rseries)
        } else {
            (rseries, lseries)
        }
    };

    let mut join_series = if lkeys
        .column_names()
        .iter()
        .zip(rkeys.column_names().iter())
        .any(|(l, r)| l == r)
    {
        let join_key_predicate = BooleanArray::from((
            "join_key_predicate",
            arrow2::array::BooleanArray::from_trusted_len_values_iter(
                lidx.u64()?
                    .into_iter()
                    .zip(ridx.u64()?)
                    .map(|(l, r)| match (l, r) {
                        (Some(_), _) => true,
                        (None, Some(_)) => false,
                        (None, None) => unreachable!("Join should not have None for both sides"),
                    }),
            ),
        ))
        .into_series();

        lkeys
            .column_names()
            .iter()
            .zip(rkeys.column_names().iter())
            .map(|(l, r)| {
                if l == r {
                    let lcol = left.get_column(l)?.take(&lidx)?;
                    let rcol = right.get_column(r)?.take(&ridx)?;

                    lcol.if_else(&rcol, &join_key_predicate)
                } else {
                    left.get_column(l)?.take(&lidx)
                }
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        left.get_columns(lkeys.column_names().as_slice())?
            .take(&lidx)?
            .columns
    };

    drop(lkeys);
    drop(rkeys);

    let num_rows = lidx.len();
    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new_with_size(join_schema, join_series, num_rows)
}
