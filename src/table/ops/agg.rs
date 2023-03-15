use crate::{datatypes::UInt64Type, dsl::Expr, error::DaftResult, series::Series, table::Table};

impl Table {
    pub fn agg(&self, _to_agg: &[(Expr, &str)], group_by: &[Expr]) -> DaftResult<Table> {
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
