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
    functions::scalar::ScalarFn,
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
                    Some(col.slice(0, 1)?.take_materialized_series()),
                    Some(col.slice(1, 2)?.take_materialized_series()),
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
            .map(|col| ColumnRangeStatistics::from_series(col.as_materialized_series()))
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
                use daft_core::prelude::Operator::*;
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
            Expr::Cast(col, dtype, try_cast) => {
                let stats = self.eval_expression(&BoundExpr::new_unchecked(col.clone()))?;
                if *try_cast {
                    Ok(stats.cast(dtype).unwrap_or(ColumnRangeStatistics::Missing))
                } else {
                    stats.cast(dtype)
                }
            }
            // `starts_with(col, prefix)` is exactly equivalent to the half-open
            // lexicographic range `prefix <= col < increment(prefix)`, so we can
            // use min/max column statistics to prune row groups / scan tasks whose
            // range does not overlap the prefix. This mirrors how mature engines
            // (e.g. Spark's `StringStartsWith`) push prefix predicates into
            // column-statistics-based data skipping.
            Expr::ScalarFn(ScalarFn::Builtin(func)) if func.name() == "starts_with" => {
                let args: Vec<&ExprRef> = func.inputs.iter().map(|arg| arg.inner()).collect();
                // Expect exactly `starts_with(input, prefix)`.
                if args.len() != 2 {
                    return Ok(ColumnRangeStatistics::Missing);
                }
                // The prefix must be a non-empty Utf8 literal; anything else
                // (column, non-string literal, empty prefix) yields no useful
                // bound, so we conservatively return Missing (never prune).
                let prefix = match args[1].as_ref() {
                    Expr::Literal(Literal::Utf8(s)) if !s.is_empty() => s.clone(),
                    _ => return Ok(ColumnRangeStatistics::Missing),
                };

                let col_stats = self.eval_expression(&BoundExpr::new_unchecked(args[0].clone()))?;

                // Lower bound: col >= prefix
                let lower: ColumnRangeStatistics = Literal::Utf8(prefix.clone()).try_into()?;
                let ge = col_stats.gte(&lower)?;

                match increment_utf8_prefix(&prefix) {
                    // Upper bound exists: col < increment(prefix)
                    Some(upper) => {
                        let upper: ColumnRangeStatistics = Literal::Utf8(upper).try_into()?;
                        let lt = col_stats.lt(&upper)?;
                        ge.bitand(&lt)
                    }
                    // Prefix is all max-value characters: no valid exclusive upper
                    // bound exists, so fall back to the lower-bound-only test. This
                    // is still sound (it just prunes less).
                    None => Ok(ge),
                }
            }
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
                if current_col_names.contains(field.name.as_ref()) {
                    // For any fields already in the table, perform a cast
                    resolved_col(field.name.clone()).cast(&field.dtype)
                } else {
                    // For any fields in schema that are not in self.schema, use fill map to fill with an expression.
                    // If no entry for column name, fall back to null literal (i.e.s create a null array for that column).
                    fill_map
                        .as_ref()
                        .and_then(|m| m.get(field.name.as_ref()))
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

/// Compute the lexicographically-next string prefix, used as the exclusive upper
/// bound when pruning with `starts_with(col, prefix)`:
/// `prefix <= col < increment_utf8_prefix(prefix)`.
///
/// It increments the right-most Unicode scalar value that can be incremented
/// (walking right to left), dropping any trailing characters already at their
/// maximum. Returns `None` when every character is at the maximum scalar value,
/// in which case the caller should fall back to a lower-bound-only range.
///
/// # UTF-8 / Unicode ordering
///
/// This operates on Unicode scalar values (chars), not bytes. Because UTF-8 byte
/// ordering matches Unicode code point ordering, incrementing a char's code point
/// produces the correct lexicographic successor for byte-wise string comparison.
///
/// Examples:
/// - `"abc"`  -> `Some("abd")`
/// - `"café"` -> `Some("cafê")`  (é U+00E9 -> ê U+00EA)
/// - `"az"` with a max last char is handled by carrying to the previous char.
fn increment_utf8_prefix(prefix: &str) -> Option<String> {
    let chars: Vec<char> = prefix.chars().collect();
    for i in (0..chars.len()).rev() {
        if let Some(next) = next_unicode_scalar(chars[i]) {
            let mut result: String = chars[..i].iter().collect();
            result.push(next);
            return Some(result);
        }
        // chars[i] is at the maximum scalar value; drop it and carry left.
    }
    // Every character was at the maximum scalar value.
    None
}

/// Return the next valid Unicode scalar value after `c`, skipping the surrogate
/// range (U+D800..=U+DFFF) which is not valid in UTF-8. Returns `None` when `c`
/// is already the maximum scalar value (U+10FFFF).
fn next_unicode_scalar(c: char) -> Option<char> {
    let next = (c as u32).checked_add(1)?;
    // Skip the surrogate range, which does not contain valid `char`s.
    let next = if (0xD800..=0xDFFF).contains(&next) {
        0xE000
    } else {
        next
    };
    char::from_u32(next)
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
    use std::sync::Arc;

    use daft_core::prelude::*;
    use daft_dsl::{Expr, expr::bound_expr::BoundExpr, lit, null_lit, resolved_col};
    use daft_functions_utf8::{endswith, startswith};
    use daft_recordbatch::RecordBatch;
    use snafu::ResultExt;

    use super::TableStatistics;
    use crate::{DaftCoreComputeSnafu, column_stats::TruthValue};

    #[test]
    fn test_equal() -> crate::Result<()> {
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 2, 3, 4]).into_series(),
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
            Int64Array::from_slice("a", &[0, 0, 0]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(resolved_col("a").eq(lit(0)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::True);

        Ok(())
    }

    #[test]
    fn test_not_null_literal_is_maybe() -> crate::Result<()> {
        // NOT null_lit() simulates a predicate on a column absent from a Parquet file
        // (schema evolution). The stats evaluator must conservatively return Maybe so
        // the row group is never incorrectly pruned.
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 2, 3]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let not_null = Arc::new(Expr::Not(null_lit()));
        let bound = BoundExpr::try_new(not_null, &table.schema).context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&bound)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_missing_string_col_eq_is_maybe() -> crate::Result<()> {
        // WHERE missing_str_col = 'foo' — non-boolean column absent from file.
        // null_lit() = lit("foo") must be Maybe (not an error, not False).
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[1, 2, 3]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let pred = Arc::new(Expr::BinaryOp {
            op: daft_core::prelude::Operator::Eq,
            left: null_lit(),
            right: lit("foo"),
        });
        let bound = BoundExpr::try_new(pred, &table.schema).context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&bound)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_missing_int_col_eq_is_maybe() -> crate::Result<()> {
        // WHERE missing_int_col = 42 — numeric column absent from file.
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("name", &["x"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let pred = Arc::new(Expr::BinaryOp {
            op: daft_core::prelude::Operator::Eq,
            left: null_lit(),
            right: lit(42i64),
        });
        let bound = BoundExpr::try_new(pred, &table.schema).context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&bound)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_range_stats_false_when_predicate_outside_range() -> crate::Result<()> {
        // Stats: min=10, max=20. Predicate: col < 5 → must be False.
        // This verifies that injected stats can trigger the MicroPartition.filter
        // short-circuit (TruthValue::False → return empty without scanning data).
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[10, 20]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        // col < 5 → False (5 < min=10)
        let expr = BoundExpr::try_new(resolved_col("a").lt(lit(5i64)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        // col > 25 → False (25 > max=20)
        let expr = BoundExpr::try_new(resolved_col("a").gt(lit(25i64)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        Ok(())
    }

    #[test]
    fn test_range_stats_maybe_when_predicate_overlaps_range() -> crate::Result<()> {
        // Stats: min=10, max=20. Predicate: col < 15 → must be Maybe.
        let table = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_slice("a", &[10, 20]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        // col < 15 → Maybe (15 is within [10, 20], some values satisfy, some don't)
        let expr = BoundExpr::try_new(resolved_col("a").lt(lit(15i64)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        // col > 12 → Maybe (12 is within [10, 20], some values satisfy, some don't)
        let expr = BoundExpr::try_new(resolved_col("a").gt(lit(12i64)), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_startswith_prunes_when_prefix_outside_range() -> crate::Result<()> {
        // Stats: min="apple", max="avocado". Predicate: starts_with(col, "b")
        // is equivalent to "b" <= col < "c"; the entire [apple, avocado] range
        // is below "b", so the row group must be pruned (False).
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["apple", "avocado"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(startswith(resolved_col("s"), lit("b")), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        Ok(())
    }

    #[test]
    fn test_startswith_maybe_when_prefix_in_range() -> crate::Result<()> {
        // Stats: min="apple", max="cherry". starts_with(col, "b") maps to the
        // range ["b", "c"), which overlaps [apple, cherry], so we cannot prune
        // and must return Maybe (some row groups may contain matches).
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["apple", "banana", "cherry"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(startswith(resolved_col("s"), lit("b")), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_startswith_all_max_prefix_falls_back_to_lower_bound() -> crate::Result<()> {
        // A prefix consisting solely of the maximum Unicode scalar has no valid
        // exclusive upper bound, so we degrade to the lower-bound-only test
        // (col >= prefix). Here max="z" < prefix, so the lower-bound test alone
        // is enough to soundly prune the group (False).
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["a", "z"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(
            startswith(resolved_col("s"), lit("\u{10FFFF}")),
            &table.schema,
        )
        .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::False);

        Ok(())
    }

    #[test]
    fn test_startswith_empty_prefix_never_prunes() -> crate::Result<()> {
        // An empty prefix matches everything, so it yields no useful bound and
        // must return Missing (Maybe) rather than pruning anything.
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["apple", "avocado"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(startswith(resolved_col("s"), lit("")), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }

    #[test]
    fn test_endswith_is_not_handled_and_stays_maybe() -> crate::Result<()> {
        // Only starts_with has a sound range mapping. ends_with (and other
        // string predicates) must fall through to Missing/Maybe so they never
        // incorrectly prune a row group.
        let table = RecordBatch::from_nonempty_columns(vec![
            Utf8Array::from_slice("s", &["apple", "avocado"]).into_series(),
        ])
        .unwrap();
        let table_stats = TableStatistics::from_table(&table);

        let expr = BoundExpr::try_new(endswith(resolved_col("s"), lit("b")), &table.schema)
            .context(DaftCoreComputeSnafu)?;
        let result = table_stats.eval_expression(&expr)?;
        assert_eq!(result.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }
}
