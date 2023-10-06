use std::ops::Not;

use daft_dsl::Expr;
use daft_table::Table;
use indexmap::IndexMap;

use crate::column_stats::ColumnStatistics;

use daft_core::array::ops::{DaftCompare, DaftLogical};

#[derive(Clone, Debug)]
pub(crate) struct TableStatistics {
    pub columns: IndexMap<String, ColumnStatistics>,
}
impl TableStatistics {
    fn from_table(table: &Table) -> Self {
        let mut columns = IndexMap::with_capacity(table.num_columns());
        for name in table.column_names() {
            let col = table.get_column(&name).unwrap();
            let stats = ColumnStatistics::from_series(col);
            columns.insert(name, stats);
        }
        TableStatistics { columns: columns }
    }
}

impl TableStatistics {
    pub(crate) fn eval_expression(&self, expr: &Expr) -> crate::Result<ColumnStatistics> {
        match expr {
            Expr::Alias(col, _) => self.eval_expression(col.as_ref()),
            Expr::Column(col) => Ok(self.columns.get(col.as_ref()).unwrap().clone()),
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
                    _ => todo!(),
                }
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries, Series,
    };
    use daft_dsl::{col, lit};
    use daft_table::Table;

    use crate::column_stats::TruthValue;

    use super::{ColumnStatistics, TableStatistics};

    #[test]
    fn test_equal() -> crate::Result<()> {
        let table =
            Table::from_columns(vec![Int64Array::from(("a", vec![1, 2, 3, 4])).into_series()])
                .unwrap();
        let table_stats = TableStatistics::from_table(&table);

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
        let table_stats = TableStatistics::from_table(&table);

        let expr = col("a").eq(&lit(0));
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::True);

        Ok(())
    }
}
