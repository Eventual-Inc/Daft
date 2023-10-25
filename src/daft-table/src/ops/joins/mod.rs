use std::collections::{HashMap, HashSet};

use daft_core::{schema::Schema, utils::supertype::try_get_supertype};

use common_error::{DaftError, DaftResult};
use daft_dsl::Expr;

use crate::Table;

mod hash_join;

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
    left_on: &[Expr],
    right_on: &[Expr],
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
    pub fn join(&self, right: &Self, left_on: &[Expr], right_on: &[Expr]) -> DaftResult<Self> {
        let join_schema = infer_join_schema(&self.schema, &right.schema, left_on, right_on)?;
        let ltable = self.eval_expression_list(left_on)?;
        let rtable = right.eval_expression_list(right_on)?;

        let (ltable, rtable) = match_types_for_tables(&ltable, &rtable)?;

        let (lidx, ridx) = hash_join::hash_inner_join(&ltable, &rtable)?;

        let mut join_fields = ltable
            .column_names()
            .iter()
            .map(|s| self.schema.get_field(s).cloned())
            .collect::<DaftResult<Vec<_>>>()?;

        let mut join_series = self
            .get_columns(ltable.column_names().as_slice())?
            .take(&lidx)?
            .columns;
        drop(ltable);
        drop(rtable);

        let mut names_so_far = HashSet::new();

        join_fields.iter().for_each(|f| {
            names_so_far.insert(f.name.clone());
        });

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
