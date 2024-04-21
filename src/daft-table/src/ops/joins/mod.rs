use std::collections::{HashMap, HashSet};

use daft_core::{
    array::growable::make_growable, schema::Schema, utils::supertype::try_get_supertype, JoinType,
    Series,
};

use common_error::{DaftError, DaftResult};
use daft_dsl::ExprRef;

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
    Ok((Table::from_columns(lseries)?, Table::from_columns(rseries)?))
}

pub fn infer_join_schema(
    left: &Schema,
    right: &Schema,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
) -> DaftResult<Schema> {
    if left_on.len() != right_on.len() {
        return Err(DaftError::ValueError(format!(
            "Length of left_on does not match length of right_on for Join {} vs {}",
            left_on.len(),
            right_on.len()
        )));
    }

    let lfields = left_on
        .iter()
        .map(|e| e.to_field(left))
        .collect::<DaftResult<Vec<_>>>()?;
    let rfields = right_on
        .iter()
        .map(|e| e.to_field(right))
        .collect::<DaftResult<Vec<_>>>()?;

    // Left Join Keys are first
    let mut join_fields = lfields
        .iter()
        .map(|f| left.get_field(&f.name).cloned())
        .collect::<DaftResult<Vec<_>>>()?;

    let left_names = lfields.iter().map(|e| e.name.as_str());
    let right_names = rfields.iter().map(|e| e.name.as_str());

    let mut names_so_far = HashSet::new();

    join_fields.iter().for_each(|f| {
        names_so_far.insert(f.name.clone());
    });

    // Then Add Left Table non-join-key columns
    for field in left.fields.values() {
        if names_so_far.contains(&field.name) {
            continue;
        } else {
            join_fields.push(field.clone());
            names_so_far.insert(field.name.clone());
        }
    }

    let zipped_names: Vec<_> = left_names.zip(right_names).map(|(l, r)| (l, r)).collect();
    let right_to_left_keys: HashMap<&str, &str> = HashMap::from_iter(zipped_names.iter().copied());

    // Then Add Right Table non-join-key columns

    for field in right.fields.values() {
        // Skip fields if they were used in the join and have the same name as the corresponding left field
        match right_to_left_keys.get(field.name.as_str()) {
            Some(val) if val.eq(&field.name.as_str()) => {
                continue;
            }
            _ => (),
        }

        let mut curr_name = field.name.clone();
        while names_so_far.contains(&curr_name) {
            curr_name = "right.".to_string() + curr_name.as_str();
        }
        join_fields.push(field.rename(curr_name.clone()));
        names_so_far.insert(curr_name.clone());
    }

    Schema::new(join_fields)
}

impl Table {
    pub fn hash_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        how: JoinType,
    ) -> DaftResult<Self> {
        self.join(right, left_on, right_on, |l, r| {
            hash_join::hash_join(l, r, how)
        })
    }

    pub fn sort_merge_join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        is_sorted: bool,
    ) -> DaftResult<Self> {
        if is_sorted {
            self.join(right, left_on, right_on, merge_join::merge_inner_join)
        } else {
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
            )?;
            left.join(&right, left_on, right_on, merge_join::merge_inner_join)
        }
    }

    fn join(
        &self,
        right: &Self,
        left_on: &[ExprRef],
        right_on: &[ExprRef],
        join_fn: impl Fn(&Table, &Table) -> DaftResult<(Series, Series)>,
    ) -> DaftResult<Self> {
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on)?;
        let ltable = self.eval_expression_list(left_on)?;
        let rtable = right.eval_expression_list(right_on)?;

        let (ltable, rtable) = match_types_for_tables(&ltable, &rtable)?;
        let (lidx, ridx) = join_fn(&ltable, &rtable)?;

        let mut join_fields = ltable
            .column_names()
            .iter()
            .map(|s| self.schema.get_field(s).cloned())
            .collect::<DaftResult<Vec<_>>>()?;

        let mut join_series = vec![];

        for (l, r) in ltable
            .column_names()
            .iter()
            .zip(rtable.column_names().iter())
        {
            if l == r {
                let lcol = self.get_column(l)?;
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

                join_series.push(growable.build()?);
            } else {
                join_series.push(self.get_column(l)?.take(&lidx)?);
            }
        }

        drop(ltable);
        drop(rtable);

        let mut names_so_far = HashSet::new();

        join_fields.iter().for_each(|f| {
            names_so_far.insert(f.name.clone());
        });

        // TODO(Clark): Parallelize with rayon.
        for field in self.schema.fields.values() {
            if names_so_far.contains(&field.name) {
                continue;
            } else {
                join_fields.push(field.clone());
                join_series.push(self.get_column(&field.name)?.take(&lidx)?);
                names_so_far.insert(field.name.clone());
            }
        }

        drop(lidx);

        // Zip the names of the left and right expressions into a HashMap
        let left_names = left_on.iter().map(|e| e.name());
        let right_names = right_on.iter().map(|e| e.name());
        let zipped_names: DaftResult<_> = left_names
            .zip(right_names)
            .map(|(l, r)| Ok((l?, r?)))
            .collect();
        let zipped_names: Vec<(&str, &str)> = zipped_names?;
        let right_to_left_keys: HashMap<&str, &str> =
            HashMap::from_iter(zipped_names.iter().copied());

        // TODO(Clark): Parallelize with Rayon.
        for field in right.schema.fields.values() {
            // Skip fields if they were used in the join and have the same name as the corresponding left field
            match right_to_left_keys.get(field.name.as_str()) {
                Some(val) if val.eq(&field.name.as_str()) => {
                    continue;
                }
                _ => (),
            }

            let mut curr_name = field.name.clone();
            while names_so_far.contains(&curr_name) {
                curr_name = "right.".to_string() + curr_name.as_str();
            }
            join_fields.push(field.rename(curr_name.clone()));
            join_series.push(
                right
                    .get_column(&field.name)?
                    .rename(curr_name.clone())
                    .take(&ridx)?,
            );
            names_so_far.insert(curr_name.clone());
        }

        drop(ridx);
        Table::new(join_schema, join_series)
    }
}
