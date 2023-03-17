use std::cmp::Ordering;

use crate::{
    array::{ops::build_multi_array_compare, BaseArray},
    datatypes::{UInt64Array, UInt64Type},
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
        //  e.g. (expr, "sum") to Expr::Agg(AggExpr::Sum(expr))
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
        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        let indices_grouped = groupby_table.sort_grouper()?;

        println!("{:?}", indices_grouped);

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let groupkeys_indices = indices_grouped
                .iter()
                .map(|a| Some(*a.first().expect("by construction")))
                .collect::<Vec<Option<u64>>>();
            let indices_as_arrow = arrow2::array::PrimitiveArray::from(groupkeys_indices);
            let indices_as_series =
                UInt64Array::from(("__TEMP_DAFT_GROUP_INDICES", Box::new(indices_as_arrow)))
                    .into_series();
            groupby_table.take(&indices_as_series)?
        };

        println!("{}", groupkeys_table);

        // Table with the aggregated values, one row for each group.
        let agged_values_table = {
            let mut subresults: Vec<Self> = vec![];

            for group_indices in indices_grouped.iter() {
                let subtable = {
                    let optional_indices = group_indices
                        .iter()
                        .map(|v| Some(*v))
                        .collect::<Vec<Option<u64>>>();
                    let indices_as_arrow = arrow2::array::PrimitiveArray::from(optional_indices);
                    let indices_as_series = UInt64Array::from((
                        "__TEMP_DAFT_GROUP_INDICES",
                        Box::new(indices_as_arrow),
                    ))
                    .into_series();
                    self.take(&indices_as_series)?
                };
                println!("{}", subtable);
                let subresult = subtable.agg_global(to_agg)?;
                println!("{}", subresult);
                subresults.push(subresult.to_owned());
            }
            Self::concat(subresults.iter().collect::<Vec<&Self>>().as_slice())?
        };

        println!("{}", agged_values_table);

        // Final result - concat the groupkey columns and the agg result columns together.
        Self::from_columns(
            [
                &groupkeys_table.columns[..],
                &agged_values_table.columns[..],
            ]
            .concat(),
        )
    }

    fn sort_grouper(&self) -> DaftResult<Vec<Vec<u64>>> {
        let argsort_series =
            Series::argsort_multikey(self.columns.as_slice(), &vec![false; self.columns.len()])?;
        let argsort_indices = argsort_series.downcast::<UInt64Type>()?.downcast();
        println!("{:?}", argsort_indices);

        let mut result_indices: Vec<Vec<u64>> = vec![];

        let comparator =
            build_multi_array_compare(self.columns.as_slice(), &vec![false; self.columns.len()])?;
        let mut maybe_curr_index: Option<usize> = None;

        for index in argsort_indices {
            let index = *index.unwrap() as usize;

            // Start a new group result if the groupkey has changed (or if there was no previous groupkey).
            match maybe_curr_index {
                None => {
                    result_indices.push(vec![]);
                    maybe_curr_index = Some(index)
                }
                Some(curr_index) => {
                    let comp_result = comparator(curr_index, index);
                    if comp_result != Ordering::Equal {
                        result_indices.push(vec![]);
                        maybe_curr_index = Some(index)
                    }
                }
            }
            result_indices.last_mut().unwrap().push(index as u64);
        }

        Ok(result_indices)
    }
}
