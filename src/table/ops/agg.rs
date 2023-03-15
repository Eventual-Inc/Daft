// use arrow2::array::Array;

use crate::{
    datatypes::{UInt64Type, Utf8Type},
    dsl::{AggExpr, Expr},
    error::DaftResult,
    series::Series,
    table::Table,
    // with_match_comparable_daft_types,
};

impl Table {
    pub fn agg(&self, to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[(Expr, &str)]) -> DaftResult<Table> {
        // Convert the input (child, name) exprs to the enum form.
        //  e.g. (expr, "sum") to Expr::Agg(AggEXpr::Sum(expr))
        // (We could do this at the pyo3 layer but I'm not sure why they're strings in the first place)
        let agg_expr_list = to_agg
            .iter()
            .map(|(e, s)| AggExpr::from_name_and_child_expr(s, e))
            .collect::<DaftResult<Vec<AggExpr>>>()?;
        let expr_list = agg_expr_list
            .iter()
            .map(|ae| Expr::Agg(ae.clone()))
            .collect::<Vec<Expr>>();
        self.eval_expression_list(&expr_list)
    }

    pub fn agg_groupby(&self, _to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        let groupby_table = self.eval_expression_list(group_by)?;

        let (_groupkeys, _indices) = groupby_table.sort_grouper()?;

        // iterate through result, manually aggregate enumerate column for each groupby row
        // for each groupby row, take the enumerate values against the table, then perform the agg
        // concat the tables together

        todo!()
    }

    fn sort_grouper(&self) -> DaftResult<(Vec<Series>, Vec<&[u64]>)> {
        let argsort_series =
            Series::argsort_multikey(self.columns.as_slice(), &vec![false; self.columns.len()])?;
        let argsort_indices = argsort_series.downcast::<UInt64Type>()?.downcast();
        println!("{:?}", argsort_indices);

        let mut result_indices: Vec<Vec<u64>> = vec![];
        let mut result_groups: Vec<Vec<Option<&str>>> = vec![];

        for index in argsort_indices {
            let i = *index.unwrap() as usize;
            let value_tuple = self
                .columns
                .iter()
                .map(|c| {
                    // todo broaden
                    let arrow_array = c
                        .downcast::<Utf8Type>()
                        .expect("Non-string types TODO")
                        .downcast();
                    match arrow_array.validity() {
                        None => Some(arrow_array.value(i)),
                        Some(bitmap) => match bitmap.get_bit(i) {
                            true => Some(arrow_array.value(i)),
                            false => None,
                        },
                    }
                })
                .collect::<Vec<Option<&str>>>();

            match result_groups.len() {
                0 => {
                    result_groups.push(value_tuple.to_owned());
                    result_indices.push(vec![]);
                }
                _ => {
                    if result_groups.last().unwrap() != &value_tuple {
                        result_groups.push(value_tuple.to_owned());
                        result_indices.push(vec![]);
                    }
                }
            }
            result_indices.last_mut().unwrap().push(*index.unwrap());
        }

        println!("{:?}", result_groups);
        println!("{:?}", result_indices);
        todo!()
    }
}
