use std::{collections::HashSet, fmt::Display, ops::Not};

use common_error::DaftError;
use daft_dsl::Expr;
use daft_table::Table;
use indexmap::{IndexMap, IndexSet};
use snafu::ResultExt;

use crate::{
    column_stats::{self, ColumnRangeStatistics},
    DaftCoreComputeSnafu,
};

use daft_core::{
    array::ops::DaftCompare,
    schema::{Schema, SchemaRef},
};

#[derive(Clone, Debug)]
pub(crate) struct TableStatistics {
    pub columns: IndexMap<String, ColumnRangeStatistics>,
}
impl TableStatistics {
    fn from_table(table: &Table) -> Self {
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
    pub(crate) fn union(&self, other: &Self) -> crate::Result<Self> {
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

    pub(crate) fn eval_expression_list(
        &self,
        exprs: &[Expr],
        schema: &Schema,
    ) -> crate::Result<Self> {
        let result_cols = exprs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<crate::Result<Vec<_>>>()?;

        let fields = exprs
            .iter()
            .map(|e| e.to_field(schema).context(DaftCoreComputeSnafu))
            .collect::<crate::Result<Vec<_>>>()?;

        let mut seen: HashSet<String> = HashSet::new();
        for field in fields.iter() {
            let name = &field.name;
            if seen.contains(name) {
                return Err(crate::Error::DuplicatedField {
                    name: name.to_string(),
                });
            }
            seen.insert(name.clone());
        }

        let new_col_stats = result_cols
            .into_iter()
            .zip(fields.into_iter())
            .map(|(c, f)| (f.name, c))
            .collect::<IndexMap<_, _>>();

        Ok(Self {
            columns: new_col_stats,
        })
    }

    pub(crate) fn eval_expression(&self, expr: &Expr) -> crate::Result<ColumnRangeStatistics> {
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
}
use crate::MissingStatisticsSnafu;

impl TryFrom<&daft_parquet::metadata::RowGroupMetaData> for TableStatistics {
    type Error = crate::Error;
    fn try_from(value: &daft_parquet::metadata::RowGroupMetaData) -> Result<Self, Self::Error> {
        let _num_rows = value.num_rows();
        let mut columns = IndexMap::new();
        for col in value.columns() {
            let stats = col
                .statistics()
                .transpose()
                .context(column_stats::UnableToParseParquetColumnStatisticsSnafu)?;
            let col_stats =
                stats.and_then(|v| v.as_ref().try_into().context(MissingStatisticsSnafu).ok());
            let col_stats = col_stats.unwrap_or(ColumnRangeStatistics::Missing);
            columns.insert(
                col.descriptor().path_in_schema.get(0).unwrap().clone(),
                col_stats,
            );
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
