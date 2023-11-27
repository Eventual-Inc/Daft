use std::{fmt::Display, ops::Not};

use daft_dsl::Expr;
use daft_table::Table;
use indexmap::{IndexMap, IndexSet};

use crate::column_stats::ColumnRangeStatistics;

use daft_core::{
    array::ops::DaftCompare,
    schema::{Schema, SchemaRef},
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableStatistics {
    pub columns: IndexMap<String, ColumnRangeStatistics>,
}

impl TableStatistics {
    fn _from_table(table: &Table) -> Self {
        let mut columns = IndexMap::with_capacity(table.num_columns());
        for name in table.column_names() {
            let col = table.get_column(&name).unwrap();
            let stats = ColumnRangeStatistics::from_series(col);
            columns.insert(name, stats);
        }
        TableStatistics { columns }
    }
}

impl TableStatistics {
    pub fn union(&self, other: &Self) -> crate::Result<Self> {
        // maybe use the schema from micropartition instead
        let unioned_columns = self
            .columns
            .keys()
            .chain(other.columns.keys())
            .collect::<IndexSet<_>>();
        let mut columns = IndexMap::with_capacity(unioned_columns.len());
        for col in unioned_columns {
            let res_col = match (self.columns.get(col), other.columns.get(col)) {
                (None, None) => panic!("Key missing from both tables; invalid state"),
                (Some(_l), None) => Ok(ColumnRangeStatistics::Missing),
                (None, Some(_r)) => Ok(ColumnRangeStatistics::Missing),
                (Some(l), Some(r)) => l.union(r),
            }?;
            columns.insert(col.clone(), res_col);
        }
        Ok(TableStatistics { columns })
    }

    pub fn eval_expression_list(
        &self,
        exprs: &[Expr],
        expected_schema: &Schema,
    ) -> crate::Result<Self> {
        let result_cols = exprs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<crate::Result<Vec<_>>>()?;

        let new_col_stats = result_cols
            .into_iter()
            .zip(expected_schema.fields.keys())
            .map(|(c, f)| (f.clone(), c))
            .collect::<IndexMap<_, _>>();

        Ok(Self {
            columns: new_col_stats,
        })
    }

    pub fn estimate_row_size(&self) -> super::Result<usize> {
        let mut sum_so_far = 0;

        for elem_size in self.columns.values().map(|c| c.element_size()) {
            sum_so_far += elem_size?;
        }
        Ok(sum_so_far)
    }

    pub fn eval_expression(&self, expr: &Expr) -> crate::Result<ColumnRangeStatistics> {
        match expr {
            Expr::Alias(col, _) => self.eval_expression(col.as_ref()),
            Expr::Column(col) => {
                let col = self.columns.get(col.as_ref()).unwrap();
                Ok(col.clone())
            }
            Expr::Literal(lit_value) => lit_value.try_into(),
            Expr::Not(col) => self.eval_expression(col)?.not(),
            Expr::BinaryOp { op, left, right } => {
                let lhs = self.eval_expression(left)?;
                let rhs = self.eval_expression(right)?;
                use daft_dsl::Operator::*;
                match op {
                    Lt => lhs.lt(&rhs),
                    LtEq => lhs.lte(&rhs),
                    Eq => lhs.equal(&rhs),
                    NotEq => lhs.not_equal(&rhs),
                    GtEq => lhs.gte(&rhs),
                    Gt => lhs.gt(&rhs),
                    Plus => &lhs + &rhs,
                    Minus => &lhs - &rhs,
                    _ => Ok(ColumnRangeStatistics::Missing),
                }
            }
            _ => Ok(ColumnRangeStatistics::Missing),
        }
    }

    pub fn cast_to_schema(&self, schema: SchemaRef) -> crate::Result<TableStatistics> {
        let mut columns = IndexMap::new();
        for (field_name, field) in schema.fields.iter() {
            let crs = match self.columns.get(field_name) {
                Some(column_stat) => column_stat
                    .cast(&field.dtype)
                    .unwrap_or(ColumnRangeStatistics::Missing),
                None => ColumnRangeStatistics::Missing,
            };
            columns.insert(field_name.clone(), crs);
        }
        Ok(TableStatistics { columns })
    }
}

impl Display for TableStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|(s, c)| c.combined_series().unwrap().rename(s))
            .collect::<Vec<_>>();

        let tab = Table::from_columns(columns).unwrap();
        write!(f, "{tab}")
    }
}

#[cfg(test)]
mod test {

    use daft_core::{datatypes::Int64Array, IntoSeries};
    use daft_dsl::{col, lit};
    use daft_table::Table;

    use crate::column_stats::TruthValue;

    use super::TableStatistics;

    #[test]
    fn test_equal() -> crate::Result<()> {
        let table =
            Table::from_columns(vec![Int64Array::from(("a", vec![1, 2, 3, 4])).into_series()])
                .unwrap();
        let table_stats = TableStatistics::_from_table(&table);

        // False case
        let expr = col("a").eq(&lit(0));
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        // Maybe case
        let expr = col("a").eq(&lit(3));
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        // True case
        let table = Table::from_columns(vec![Int64Array::from(("a", vec![0, 0, 0])).into_series()])
            .unwrap();
        let table_stats = TableStatistics::_from_table(&table);

        let expr = col("a").eq(&lit(0));
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::True);

        Ok(())
    }
}
