use crate::{dsl::Expr, error::DaftResult, series::Series, table::Table};

impl Table {
    pub fn agg(&self, _to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
        let groupby_table = self.eval_expression_list(group_by)?;
        let argsorted = Series::argsort_multikey(
            groupby_table.columns.as_slice(),
            &vec![true; group_by.len()],
        )?;
        println!("{}", argsorted);

        // add enumerate column 0..n
        // argsort on groupby cols
        // iterate through result, manually aggregate enumerate column for each groupby row
        // for each groupby row, take the enumerate values against the table, then perform the agg
        // concat the tables together

        todo!()
    }
    /*
    pub fn sort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Table> {
        let argsort = self.argsort(sort_keys, descending)?;
        self.take(&argsort)
    }

    pub fn argsort(&self, sort_keys: &[Expr], descending: &[bool]) -> DaftResult<Series> {
        if sort_keys.len() != descending.len() {
            return Err(DaftError::ValueError(format!(
                "sort_keys and descending length must match, got {} vs {}",
                sort_keys.len(),
                descending.len()
            )));
        }
        if sort_keys.len() == 1 {
            self.eval_expression(sort_keys.get(0).unwrap())?
                .argsort(*descending.first().unwrap())
        } else {
            let expr_result = self.eval_expression_list(sort_keys)?;
            Series::argsort_multikey(expr_result.columns.as_slice(), descending)
        }
    }*/
}
