use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    ops::{BitAnd, BitOr, Index, Not},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    Column, Expr, ExprRef,
    expr::{BoundColumn, bound_expr::BoundExpr},
    null_lit, resolved_col,
};
use daft_recordbatch::RecordBatch;
use snafu::ResultExt;

use crate::{DaftCoreComputeSnafu, column_stats::ColumnRangeStatistics};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Hash)]
pub struct TableStatistics {
    columns: Vec<ColumnRangeStatistics>,
    schema: SchemaRef,
}

impl TableStatistics {
    pub fn new(columns: Vec<ColumnRangeStatistics>, schema: SchemaRef) -> Self {
        Self { columns, schema }
    }

    pub fn from_stats_table(table: &RecordBatch) -> DaftResult<Self> {
        // Assumed format is each column having 2 rows:
        // - row 0: Minimum value for the column.
        // - row 1: Maximum value for the column.
        if table.len() != 2 {
            return Err(DaftError::ValueError(format!(
                "Expected stats table to have 2 rows, with min and max values for each column, but got {} rows: {}",
                table.len(),
                table
            )));
        }
        let columns = table
            .columns()
            .iter()
            .map(|col| {
                Ok(ColumnRangeStatistics::new(
                    Some(col.slice(0, 1)?),
                    Some(col.slice(1, 2)?),
                )?)
            })
            .collect::<DaftResult<_>>()?;

        Ok(Self {
            columns,
            schema: table.schema.clone(),
        })
    }

    #[must_use]
    pub fn from_table(table: &RecordBatch) -> Self {
        let columns = table
            .columns()
            .iter()
            .map(ColumnRangeStatistics::from_series)
            .collect();
        Self {
            columns,
            schema: table.schema.clone(),
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn union(&self, other: &Self) -> crate::Result<Self> {
        if self.schema != other.schema {
            return Err(crate::Error::DaftCoreCompute {
                source: DaftError::SchemaMismatch(format!(
                    "TableStatistics::union requires schemas to match, found: {} vs {}",
                    self.schema, other.schema
                )),
            });
        }

        let columns = self
            .columns
            .iter()
            .zip(other.columns.iter())
            .map(|(l, r)| l.union(r))
            .collect::<crate::Result<_>>()?;

        Ok(Self {
            columns,
            schema: self.schema.clone(),
        })
    }

    pub fn eval_expression_list(&self, exprs: &[BoundExpr]) -> crate::Result<Self> {
        let columns = exprs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<crate::Result<Vec<_>>>()?;

        let schema = Arc::new(Schema::new(
            exprs
                .iter()
                .map(|e| e.inner().to_field(&self.schema))
                .collect::<DaftResult<Vec<_>>>()
                .context(DaftCoreComputeSnafu)?,
        ));

        Ok(Self { columns, schema })
    }

    pub fn estimate_row_size(&self) -> super::Result<f64> {
        self.columns
            .iter()
            .filter_map(|col| col.element_size().transpose())
            .sum()
    }

    pub fn eval_expression(&self, expr: &BoundExpr) -> crate::Result<ColumnRangeStatistics> {
        match expr.as_ref() {
            Expr::Alias(col, _) => self.eval_expression(&BoundExpr::new_unchecked(col.clone())),
            Expr::Column(Column::Bound(BoundColumn { index, .. })) => {
                Ok(self.columns[*index].clone())
            }
            Expr::Literal(lit_value) => lit_value.clone().try_into(),
            Expr::Not(col) => self
                .eval_expression(&BoundExpr::new_unchecked(col.clone()))?
                .not(),
            Expr::BinaryOp { op, left, right } => {
                let lhs = self.eval_expression(&BoundExpr::new_unchecked(left.clone()))?;
                let rhs = self.eval_expression(&BoundExpr::new_unchecked(right.clone()))?;
                use daft_dsl::Operator::{And, Eq, Gt, GtEq, Lt, LtEq, Minus, NotEq, Or, Plus};
                match op {
                    Lt => lhs.lt(&rhs),
                    LtEq => lhs.lte(&rhs),
                    Eq => lhs.equal(&rhs),
                    NotEq => lhs.not_equal(&rhs),
                    GtEq => lhs.gte(&rhs),
                    Gt => lhs.gt(&rhs),
                    Plus => &lhs + &rhs,
                    Minus => &lhs - &rhs,
                    And => lhs.bitand(&rhs),
                    Or => lhs.bitor(&rhs),
                    _ => Ok(ColumnRangeStatistics::Missing),
                }
            }
            Expr::Cast(col, dtype) => self
                .eval_expression(&BoundExpr::new_unchecked(col.clone()))?
                .cast(dtype),
            _ => Ok(ColumnRangeStatistics::Missing),
        }
    }

    #[deprecated(note = "name-referenced columns")]
    /// Casts a `TableStatistics` to a schema.
    ///
    /// Note: this method is deprecated because it maps fields by name, which will not work for schemas with duplicate field names.
    /// It should only be used for scans, and once we support reading files with duplicate column names, we should remove this function.
    pub fn cast_to_schema(&self, schema: &Schema) -> crate::Result<Self> {
        #[allow(deprecated)]
        self.cast_to_schema_with_fill(schema, None)
    }

    #[deprecated(note = "name-referenced columns")]
    /// Casts a `TableStatistics` to a schema, using `fill_map` to specify the default expression for a column that doesn't exist.
    ///
    /// Note: this method is deprecated because it maps fields by name, which will not work for schemas with duplicate field names.
    /// It should only be used for scans, and once we support reading files with duplicate column names, we should remove this function.
    pub fn cast_to_schema_with_fill(
        &self,
        schema: &Schema,
        fill_map: Option<&HashMap<&str, ExprRef>>,
    ) -> crate::Result<Self> {
        let current_col_names = HashSet::<_>::from_iter(self.schema.field_names());
        let null_lit = null_lit();
        let exprs: Vec<_> = schema
            .into_iter()
            .map(|field| {
                if current_col_names.contains(field.name.as_str()) {
                    // For any fields already in the table, perform a cast
                    resolved_col(field.name.clone()).cast(&field.dtype)
                } else {
                    // For any fields in schema that are not in self.schema, use fill map to fill with an expression.
                    // If no entry for column name, fall back to null literal (i.e.s create a null array for that column).
                    fill_map
                        .as_ref()
                        .and_then(|m| m.get(field.name.as_str()))
                        .unwrap_or(&null_lit)
                        .clone()
                        .alias(field.name.clone())
                        .cast(&field.dtype)
                }
            })
            .map(|expr| BoundExpr::try_new(expr, &self.schema))
            .collect::<DaftResult<_>>()
            .context(DaftCoreComputeSnafu)?;
        self.eval_expression_list(&exprs)
    }
}

impl Display for TableStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns = self
            .columns
            .iter()
            .zip(self.schema.as_ref())
            .map(|(c, s)| c.combined_series().unwrap().rename(&s.name))
            .collect::<Vec<_>>();
        let tbl_schema = Schema::new(columns.iter().map(|s| s.field().clone()));
        let tab = RecordBatch::new_with_size(tbl_schema, columns, 2).unwrap();
        write!(f, "{tab}")
    }
}

impl Index<usize> for TableStatistics {
    type Output = ColumnRangeStatistics;

    fn index(&self, index: usize) -> &Self::Output {
        &self.columns[index]
    }
}

impl<'a> IntoIterator for &'a TableStatistics {
    type Item = &'a ColumnRangeStatistics;
    type IntoIter = std::slice::Iter<'a, ColumnRangeStatistics>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter()
    }
}

#[cfg(test)]
mod test {
    use daft_core::prelude::*;
    use daft_dsl::{expr::bound_expr::BoundExpr, lit, resolved_col};
    use daft_recordbatch::RecordBatch;
    use snafu::ResultExt;

    use super::TableStatistics;
    use crate::{DaftCoreComputeSnafu, column_stats::TruthValue};

    #[test]
    fn test_equal() -> crate::Result<()> {
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from(("a", vec![1, 2, 3, 4])).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        // False case
        let expr = BoundExpr::try_new(resolved_col("a").eq(lit(0)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        // Maybe case
        let expr = BoundExpr::try_new(resolved_col("a").eq(lit(3)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        // True case
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from(("a", vec![0, 0, 0])).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(resolved_col("a").eq(lit(0)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::True);

        Ok(())
    }
}
