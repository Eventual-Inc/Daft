use crate::{
    datatypes::UInt64Type,
    dsl::{AggExpr, Expr},
    error::DaftResult,
    series::Series,
    table::Table,
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

    pub fn agg_groupby(&self, to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        let groupby_table = self.eval_expression_list(group_by)?;
        // Inverse argsort returns a list of indices, e.g. ["b", "c", "a"] -> [2, 0, 1]
        // It is used to traverse the original array in sorted order.
        // e.g. [2, 0, 1] -> [2nd, 0th, 1st] -> ["a", "b", "c"]
        let inverse_argsort = {
            let argsort_series = Series::argsort_multikey(
                groupby_table.columns.as_slice(),
                &vec![false; group_by.len()],
            )?;
            let argsort_indices = argsort_series.downcast::<UInt64Type>()?.downcast();
            let mut temp = argsort_indices
                .values_iter()
                .enumerate()
                .map(|(i, v)| (v, i))
                .collect::<Vec<_>>();
            temp.sort();
            temp.iter().map(|(_, v)| *v).collect::<Vec<_>>()
        };
        println!("{:?}", inverse_argsort);
        // add enumerate column 0..n
        // argsort on groupby cols
        // iterate through result, manually aggregate enumerate column for each groupby row
        // for each groupby row, take the enumerate values against the table, then perform the agg
        // concat the tables together

        todo!()
    }
}
