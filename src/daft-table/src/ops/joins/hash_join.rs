use std::iter::repeat;

use arrow2::{bitmap::MutableBitmap, types::IndexRange};
use daft_core::{
    array::{
        growable::make_growable,
        ops::{arrow2::comparison::build_multi_array_is_equal, full::FullNull},
    },
    datatypes::UInt64Array,
    DataType, IntoSeries,
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
    let join_schema = infer_join_schema(&left.schema, &right.schema, left_on, right_on)?;
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

    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new(join_schema, join_series)
}

pub(super) fn hash_left_right_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
    left_side: bool,
) -> DaftResult<Table> {
    let join_schema = infer_join_schema(&left.schema, &right.schema, left_on, right_on)?;
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

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        let mut l_valid = MutableBitmap::new();

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

    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new(join_schema, join_series)
}

pub(super) fn hash_outer_join(
    left: &Table,
    right: &Table,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
) -> DaftResult<Table> {
    let join_schema = infer_join_schema(&left.schema, &right.schema, left_on, right_on)?;
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

        let mut left_idx = vec![];
        let mut right_idx = vec![];

        let mut l_valid = MutableBitmap::new();
        let mut r_valid = MutableBitmap::new();

        let mut left_idx_used = vec![false; lkeys.len()];

        for (r_idx, h) in r_hashes.as_arrow().values_iter().enumerate() {
            if let Some((_, indices)) = probe_table.raw_entry().from_hash(*h, |other| {
                *h == other.hash && {
                    let l_idx = other.idx;
                    is_equal(l_idx as usize, r_idx)
                }
            }) {
                for l_idx in indices {
                    left_idx.push(*l_idx);
                    left_idx_used[*l_idx as usize] = true;

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

        for (l_idx, used) in left_idx_used.iter().enumerate() {
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

    let mut join_series = lkeys
        .column_names()
        .iter()
        .zip(rkeys.column_names().iter())
        .map(|(l, r)| {
            if l == r {
                let lcol = left.get_column(l)?;
                let rcol = right.get_column(r)?;

                let mut growable =
                    make_growable(l, lcol.data_type(), vec![lcol, rcol], false, lcol.len());

                for (li, ri) in lidx.u64()?.into_iter().zip(ridx.u64()?) {
                    match (li, ri) {
                        (Some(i), _) => growable.extend(0, *i as usize, 1),
                        (None, Some(i)) => growable.extend(1, *i as usize, 1),
                        (None, None) => unreachable!("Join should not have None for both sides"),
                    }
                }

                growable.build()
            } else {
                left.get_column(l)?.take(&lidx)
            }
        })
        .collect::<DaftResult<Vec<_>>>()?;

    drop(lkeys);
    drop(rkeys);

    join_series =
        add_non_join_key_columns(left, right, lidx, ridx, left_on, right_on, join_series)?;

    Table::new(join_schema, join_series)
}
