use std::collections::HashSet;

use crate::{dsl::Expr, error::DaftResult, schema::Schema, table::Table};

mod naive_join;

impl Table {
    pub fn join(&self, right: &Self, left_on: &[Expr], right_on: &[Expr]) -> DaftResult<Self> {
        let ltable = self.eval_expression_list(left_on)?;
        let rtable = self.eval_expression_list(right_on)?;

        let (lidx, ridx) = naive_join::naive_inner_join(&ltable, &rtable)?;

        let mut join_fields = ltable
            .schema
            .fields
            .clone()
            .into_values()
            .collect::<Vec<_>>();

        let mut join_series = ltable.take(&lidx)?.columns.clone();

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

        for field in right.schema.fields.values() {
            if ltable.get_column(&field.name).is_ok() {
                continue;
            }
            let mut curr_name = field.name.clone();
            while names_so_far.contains(&curr_name) {
                curr_name = "right.".to_string() + &curr_name;
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

        return Table::new(Schema::new(join_fields), join_series);
    }
}
