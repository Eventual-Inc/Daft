// use arrow2::array::Array;

use crate::{
    array::{BaseArray},
    datatypes::{UInt64Array, UInt64Type, Utf8Type},
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
        let groupby_table = self.eval_expression_list(group_by)?;

        let (groupkeys, indices) = groupby_table.sort_grouper()?;

        println!("{:?}", groupkeys);
        println!("{:?}", indices);

        // for each groupby row, take the indices against the table, then perform the agg
        // concat the tables together

        let agged_values = {
            let mut subresults: Vec<Self> = vec![];

            for group_indices in indices.iter() {
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
            Self::concat(
                subresults
                    .iter()
                    .collect::<Vec<&Self>>()
                    .as_slice(),
            )?
        };

        println!("{}", agged_values);

        let groupkeys_table = {
            let groupkeys_indices = indices
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

        Self::from_columns([&groupkeys_table.columns[..], &agged_values.columns[..]].concat())
    }

    fn sort_grouper(&self) -> DaftResult<(Vec<Vec<Option<&str>>>, Vec<Vec<u64>>)> {
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

        Ok((result_groups, result_indices))
    }
}
