use std::collections::HashSet;

use common_error::{DaftError, DaftResult};
use daft_core::{array::growable::make_growable, prelude::*, utils::supertype::try_get_supertype};
use daft_dsl::{
    join::{get_common_join_keys, infer_join_schema},
    ExprRef,
};
use hash_join::hash_semi_anti_join;

use self::hash_join::{hash_inner_join, hash_left_right_join, hash_outer_join};
use crate::Table;
mod hash_join;
mod merge_join;

fn match_types_for_tables(left: &Table, right: &Table) -> DaftResult<(Table, Table)> {
    let mut lseries = vec![];
    let mut rseries = vec![];

    for (ls, rs) in left.columns.iter().zip(right.columns.iter()) {
        let st = try_get_supertype(ls.data_type(), rs.data_type());
        if let Ok(st) = st {
            lseries.push(ls.cast(&st)?);
            rseries.push(rs.cast(&st)?);
        } else {
            return Err(DaftError::SchemaMismatch(format!(
                "Can not perform join between due to mismatch of types of left: {} vs right: {}",
                ls.field(),
                rs.field()
            )));
        }
    }
    Ok((
        Table::from_nonempty_columns(lseries)?,
        Table::from_nonempty_columns(rseries)?,
    ))
}

fn add_non_join_key_columns(
    left: &Table,
    right: &Table,
    lidx: Series,
    ridx: Series,
    mut join_series: Vec<Series>,
) -> DaftResult<Vec<Series>> {
    let join_keys = join_series
        .iter()
        .map(|s| s.name().to_string())
        .collect::<HashSet<_>>();

    // TODO(Clark): Parallelize with rayon.
    for field in left.schema.fields.values() {
        if join_keys.contains(&field.name) {
            continue;
        }
        join_series.push(left.get_column(&field.name)?.take(&lidx)?);
    }

    drop(lidx);

    for field in right.schema.fields.values() {
        if join_keys.contains(&field.name) {
            continue;
        }

        join_series.push(right.get_column(&field.name)?.take(&ridx)?);
    }

    Ok(join_series)
}

impl Table {
    pub fn hash_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        null_equals_nulls: &[bool],
        how: JoinType,
    ) -> DaftResult<Self> {
        if left_on.len() != right_on.len() {
            return Err(DaftError::ValueError(format!(
                "Mismatch of join on clauses: left: {:?} vs right: {:?}",
                left_on.len(),
                right_on.len()
            )));
        }

        if left_on.is_empty() {
            return Err(DaftError::ValueError(
                "No columns were passed in to join on".to_string(),
            ));
        }

        match how {
            JoinType::Inner => hash_inner_join(self, right, left_on, right_on, null_equals_nulls),
            JoinType::Left => {
                hash_left_right_join(self, right, left_on, right_on, null_equals_nulls, true)
            }
            JoinType::Right => {
                hash_left_right_join(self, right, left_on, right_on, null_equals_nulls, false)
            }
            JoinType::Outer => hash_outer_join(self, right, left_on, right_on, null_equals_nulls),
            JoinType::Semi => {
                hash_semi_anti_join(self, right, left_on, right_on, null_equals_nulls, false)
            }
            JoinType::Anti => {
                hash_semi_anti_join(self, right, left_on, right_on, null_equals_nulls, true)
            }
        }
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        is_sorted: bool,
    ) -> DaftResult<Self> {
        // sort first and then call join recursively
        if !is_sorted {
            if left_on.is_empty() {
                return Err(DaftError::ValueError(
                    "No columns were passed in to join on".to_string(),
                ));
            }
            let left = self.sort(
                left_on,
                std::iter::repeat(false)
                    .take(left_on.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
                std::iter::repeat(false)
                    .take(left_on.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?;
            if right_on.is_empty() {
                return Err(DaftError::ValueError(
                    "No columns were passed in to join on".to_string(),
                ));
            }
            let right = right.sort(
                right_on,
                std::iter::repeat(false)
                    .take(right_on.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
                std::iter::repeat(false)
                    .take(right_on.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?;

            return left.sort_merge_join(&right, left_on, right_on, true);
        }

        let join_schema = infer_join_schema(
            &self.schema,
            &right.schema,
            left_on,
            right_on,
            JoinType::Inner,
        )?;
        let ltable = self.eval_expression_list(left_on)?;
        let rtable = right.eval_expression_list(right_on)?;

        let (ltable, rtable) = match_types_for_tables(&ltable, &rtable)?;
        let (lidx, ridx) = merge_join::merge_inner_join(&ltable, &rtable)?;

        let mut join_series = get_common_join_keys(left_on, right_on)
            .map(|name| {
                let lcol = self.get_column(name)?;
                let rcol = right.get_column(name)?;

                let mut growable =
                    make_growable(name, lcol.data_type(), vec![lcol, rcol], false, lcol.len());

                for (li, ri) in lidx.u64()?.into_iter().zip(ridx.u64()?) {
                    match (li, ri) {
                        (Some(i), _) => growable.extend(0, *i as usize, 1),
                        (None, Some(i)) => growable.extend(1, *i as usize, 1),
                        (None, None) => unreachable!("Join should not have None for both sides"),
                    }
                }

                growable.build()
            })
            .collect::<DaftResult<Vec<_>>>()?;

        drop(ltable);
        drop(rtable);

        let num_rows = lidx.len();
        join_series = add_non_join_key_columns(self, right, lidx, ridx, join_series)?;

        Self::new_with_size(join_schema, join_series, num_rows)
    }
}
