#![feature(hash_raw_entry)]
#![feature(let_chains)]

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter, Result};

use daft_core::array::ops::full::FullNull;
use daft_core::utils::display_table::make_comfy_table;
use num_traits::ToPrimitive;

use daft_core::array::ops::GroupIndices;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::{BooleanArray, DataType, Field, UInt64Array};
use daft_core::schema::{Schema, SchemaRef};
use daft_core::series::{IntoSeries, Series};

use daft_dsl::functions::FunctionEvaluator;
use daft_dsl::{col, null_lit, AggExpr, ApproxPercentileParams, Expr, ExprRef, LiteralValue};
#[cfg(feature = "python")]
pub mod ffi;
mod ops;

pub use ops::infer_join_schema;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Table {
    pub schema: SchemaRef,
    columns: Vec<Series>,
    num_rows: usize,
}

#[inline]
fn _validate_schema(schema: &Schema, columns: &[Series]) -> DaftResult<()> {
    if schema.fields.len() != columns.len() {
        return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the number of fields did not match between the schema and the input columns.\n {:?}\n vs\n {:?}", schema.fields.len(), columns.len())));
    }
    for (field, series) in schema.fields.values().zip(columns.iter()) {
        if field != series.field() {
            return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the Schema Field and the Series Field  did not match. schema field: {field} vs series field: {}", series.field())));
        }
    }
    Ok(())
}

impl Table {
    /// Create a new [`Table`] and handle broadcasting of any unit-length columns
    ///
    /// Note that this function is slow. You might instead be looking for [`Table::new_with_size`] which does not perform broadcasting
    /// or [`Table::new_unchecked`] if you've already performed your own validation logic.
    ///
    /// # Arguments
    ///
    /// * `schema` - Expected [`Schema`] of the new [`Table`], used for validation
    /// * `columns` - Columns to crate a table from as [`Series`] objects
    /// * `num_rows` - Expected number of rows in the [`Table`], passed explicitly to handle cases where `columns` is empty
    pub fn new_with_broadcast<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> DaftResult<Self> {
        let schema: SchemaRef = schema.into();
        _validate_schema(schema.as_ref(), columns.as_slice())?;

        // Validate Series lengths against provided num_rows
        for (field, series) in schema.fields.values().zip(columns.iter()) {
            if (series.len() != 1) && (series.len() != num_rows) {
                return Err(DaftError::ValueError(format!("While building a Table with Table::new_with_broadcast, we found that the Series lengths did not match and could not be broadcasted. Series named: {} had length: {} vs the specified Table length: {}", field.name, series.len(), num_rows)));
            }
        }

        // Broadcast any unit-length Series
        let columns: DaftResult<Vec<Series>> = columns
            .into_iter()
            .map(|s| {
                if s.len() == num_rows {
                    Ok(s)
                } else {
                    s.broadcast(num_rows)
                }
            })
            .collect();

        Ok(Table::new_unchecked(schema, columns?, num_rows))
    }

    /// Create a new [`Table`] and validate against `num_rows`
    ///
    /// Note that this function is slow. You might instead be looking for [`Table::new_unchecked`] if you've already performed your own validation logic.
    ///
    /// # Arguments
    ///
    /// * `schema` - Expected [`Schema`] of the new [`Table`], used for validation
    /// * `columns` - Columns to crate a table from as [`Series`] objects
    /// * `num_rows` - Expected number of rows in the [`Table`], passed explicitly to handle cases where `columns` is empty
    pub fn new_with_size<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> DaftResult<Self> {
        let schema: SchemaRef = schema.into();
        _validate_schema(schema.as_ref(), columns.as_slice())?;

        // Validate Series lengths against provided num_rows
        for (field, series) in schema.fields.values().zip(columns.iter()) {
            if series.len() != num_rows {
                return Err(DaftError::ValueError(format!("While building a Table with Table::new_with_size, we found that the Series lengths did not match. Series named: {} had length: {} vs the specified Table length: {}", field.name, series.len(), num_rows)));
            }
        }

        Ok(Table::new_unchecked(schema, columns, num_rows))
    }

    /// Create a new [`Table`] without any validations
    ///
    /// WARNING: be sure that your data is valid, or this might cause downstream problems such as segfaults!
    ///
    /// This constructor is meant to be used from code that needs very fast performance (e.g. I/O code) and
    /// already does its own validations.
    pub fn new_unchecked<S: Into<SchemaRef>>(
        schema: S,
        columns: Vec<Series>,
        num_rows: usize,
    ) -> Self {
        Table {
            schema: schema.into(),
            columns,
            num_rows,
        }
    }

    pub fn empty(schema: Option<SchemaRef>) -> DaftResult<Self> {
        let schema = schema.unwrap_or_else(|| Schema::empty().into());
        let mut columns: Vec<Series> = Vec::with_capacity(schema.names().len());
        for (field_name, field) in schema.fields.iter() {
            let series = Series::empty(field_name, &field.dtype);
            columns.push(series)
        }
        Ok(Table::new_unchecked(schema, columns, 0))
    }

    /// Create a Table from a set of columns.
    ///
    /// Note: `columns` cannot be empty (will panic if so.) and must all have the same length.
    ///
    /// # Arguments
    ///
    /// * `columns` - Columns to crate a table from as [`Series`] objects
    pub fn from_nonempty_columns(columns: Vec<Series>) -> DaftResult<Self> {
        if columns.is_empty() {
            panic!("Cannot call Table::new() with empty columns. This indicates an internal error, please file an issue.");
        }

        let schema = Schema::new(columns.iter().map(|s| s.field().clone()).collect())?;
        let schema: SchemaRef = schema.into();
        _validate_schema(schema.as_ref(), columns.as_slice())?;

        // Infer the num_rows, assume no broadcasting
        let mut num_rows = 1;
        for (field, series) in schema.fields.values().zip(columns.iter()) {
            if num_rows == 1 {
                num_rows = series.len();
            }
            if series.len() != num_rows {
                return Err(DaftError::ValueError(format!("While building a Table with Table::new_with_nonempty_columns, we found that the Series lengths did not match. Series named: {} had length: {} vs inferred Table length: {}", field.name, series.len(), num_rows)));
            }
        }

        Ok(Table::new_unchecked(schema, columns, num_rows))
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> =
            self.columns.iter().map(|s| s.slice(start, end)).collect();
        let new_num_rows = self.len().min(end - start);
        Table::new_with_size(self.schema.clone(), new_series?, new_num_rows)
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            return Ok(Table::new_unchecked(
                self.schema.clone(),
                self.columns.clone(),
                self.len(),
            ));
        }
        self.slice(0, num)
    }

    pub fn sample_by_fraction(
        &self,
        fraction: f64,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        let num = (fraction * self.len() as f64).ceil() as usize;
        self.sample(num, with_replacement, seed)
    }

    pub fn sample(
        &self,
        num: usize,
        with_replacement: bool,
        seed: Option<u64>,
    ) -> DaftResult<Self> {
        if num >= self.len() {
            Ok(self.clone())
        } else {
            use rand::{distributions::Uniform, rngs::StdRng, Rng, SeedableRng};
            let mut rng = match seed {
                Some(seed) => StdRng::seed_from_u64(seed),
                None => StdRng::from_rng(rand::thread_rng()).unwrap(),
            };
            let range = Uniform::from(0..self.len() as u64);
            let values: Vec<u64> = if with_replacement {
                (0..num).map(|_| rng.sample(range)).collect()
            } else {
                rng.sample_iter(&range).take(num).collect()
            };
            let indices: daft_core::array::DataArray<daft_core::datatypes::UInt64Type> =
                UInt64Array::from(("idx", values));
            self.take(&indices.into_series())
        }
    }

    pub fn add_monotonically_increasing_id(
        &self,
        partition_num: u64,
        offset: u64,
        column_name: &str,
    ) -> DaftResult<Self> {
        // Use the leftmost 28 bits for the partition number and the rightmost 36 bits for the row number
        let start = (partition_num << 36) + offset;
        let end = start + self.len() as u64;
        let ids = (start..end).step_by(1).collect::<Vec<_>>();
        let id_series = UInt64Array::from((column_name, ids)).into_series();
        Self::from_nonempty_columns([&[id_series], &self.columns[..]].concat())
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        if self.is_empty() {
            return Ok(self.clone());
        }

        if num == 0 {
            let indices = UInt64Array::empty("idx", &DataType::UInt64);
            return self.take(&indices.into_series());
        }

        let self_len = self.len();

        let sample_points: Vec<u64> = (1..num)
            .map(|i| {
                ((i as f64 / num as f64) * self_len as f64)
                    .floor()
                    .to_u64()
                    .unwrap()
                    .min((self.len() - 1) as u64)
            })
            .collect();
        let indices = UInt64Array::from(("idx", sample_points));
        self.take(&indices.into_series())
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        let column_sizes: DaftResult<Vec<usize>> =
            self.columns.iter().map(|s| s.size_bytes()).collect();
        Ok(column_sizes?.iter().sum())
    }

    pub fn filter(&self, predicate: &[ExprRef]) -> DaftResult<Self> {
        if predicate.is_empty() {
            Ok(self.clone())
        } else if predicate.len() == 1 {
            let mask = self.eval_expression(predicate.first().unwrap().as_ref())?;
            self.mask_filter(&mask)
        } else {
            let mut expr = predicate
                .first()
                .unwrap()
                .clone()
                .and(predicate.get(1).unwrap().clone());
            for i in 2..predicate.len() {
                let next = predicate.get(i).unwrap();
                expr = expr.and(next.clone());
            }
            let mask = self.eval_expression(&expr)?;
            self.mask_filter(&mask)
        }
    }

    pub fn mask_filter(&self, mask: &Series) -> DaftResult<Self> {
        if *mask.data_type() != DataType::Boolean {
            return Err(DaftError::ValueError(format!(
                "We can only mask a Table with a Boolean Series, but we got {}",
                mask.data_type()
            )));
        }

        let mask = mask.downcast::<BooleanArray>().unwrap();
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.filter(mask)).collect();

        // The number of rows post-filter should be the number of 'true' values in the mask
        let num_rows = if mask.len() == 1 {
            // account for broadcasting of mask
            if mask.get(0).is_some_and(|b| b) {
                self.len()
            } else {
                0
            }
        } else {
            // num_filtered is the number of 'false' or null values in the mask
            let num_filtered = mask
                .validity()
                .map(|validity| arrow2::bitmap::and(validity, mask.as_bitmap()).unset_bits())
                .unwrap_or(mask.as_bitmap().unset_bits());
            mask.len() - num_filtered
        };

        Table::new_with_size(self.schema.clone(), new_series?, num_rows)
    }

    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.take(idx)).collect();
        Table::new_with_size(self.schema.clone(), new_series?, idx.len())
    }

    pub fn concat<T: AsRef<Table>>(tables: &[T]) -> DaftResult<Self> {
        if tables.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 Table to perform concat".to_string(),
            ));
        }
        if tables.len() == 1 {
            return Ok((*tables.first().unwrap().as_ref()).clone());
        }
        let first_table = tables.first().unwrap().as_ref();

        let first_schema = first_table.schema.as_ref();
        for tab in tables.iter().skip(1).map(|t| t.as_ref()) {
            if tab.schema.as_ref() != first_schema {
                return Err(DaftError::SchemaMismatch(format!(
                    "Table concat requires all schemas to match, {} vs {}",
                    first_schema, tab.schema
                )));
            }
        }
        let num_columns = first_table.num_columns();
        let mut new_series = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let series_to_cat: Vec<&Series> = tables
                .iter()
                .map(|s| s.as_ref().get_column_by_index(i).unwrap())
                .collect();
            new_series.push(Series::concat(series_to_cat.as_slice())?);
        }

        Table::new_with_size(
            first_table.schema.clone(),
            new_series,
            tables.iter().map(|t| t.as_ref().len()).sum(),
        )
    }

    pub fn get_column<S: AsRef<str>>(&self, name: S) -> DaftResult<&Series> {
        let i = self.schema.get_index(name.as_ref())?;
        Ok(self.columns.get(i).unwrap())
    }

    pub fn get_columns<S: AsRef<str>>(&self, names: &[S]) -> DaftResult<Self> {
        let series_by_name = names
            .iter()
            .map(|s| self.get_column(s).cloned())
            .collect::<DaftResult<Vec<_>>>()?;
        Self::new_with_size(
            Schema::new(series_by_name.iter().map(|s| s.field().clone()).collect())?,
            series_by_name,
            self.len(),
        )
    }

    pub fn get_column_by_index(&self, idx: usize) -> DaftResult<&Series> {
        Ok(self.columns.get(idx).unwrap())
    }

    fn eval_agg_expression(
        &self,
        agg_expr: &AggExpr,
        groups: Option<&GroupIndices>,
    ) -> DaftResult<Series> {
        use daft_dsl::AggExpr::*;
        match agg_expr {
            Count(expr, mode) => Series::count(&self.eval_expression(expr)?, groups, *mode),
            Sum(expr) => Series::sum(&self.eval_expression(expr)?, groups),
            ApproxSketch(expr) => Series::approx_sketch(&self.eval_expression(expr)?, groups),
            ApproxPercentile(ApproxPercentileParams {
                child: expr,
                percentiles,
                force_list_output,
            }) => {
                let percentiles = percentiles.iter().map(|p| p.0).collect::<Vec<f64>>();
                Series::approx_sketch(&self.eval_expression(expr)?, groups)?
                    .sketch_percentile(&percentiles, *force_list_output)
            }
            MergeSketch(expr) => Series::merge_sketch(&self.eval_expression(expr)?, groups),
            Mean(expr) => Series::mean(&self.eval_expression(expr)?, groups),
            Min(expr) => Series::min(&self.eval_expression(expr)?, groups),
            Max(expr) => Series::max(&self.eval_expression(expr)?, groups),
            AnyValue(expr, ignore_nulls) => {
                Series::any_value(&self.eval_expression(expr)?, groups, *ignore_nulls)
            }
            List(expr) => Series::agg_list(&self.eval_expression(expr)?, groups),
            Concat(expr) => Series::agg_concat(&self.eval_expression(expr)?, groups),
            MapGroups { .. } => Err(DaftError::ValueError(
                "MapGroups not supported via aggregation, use map_groups instead".to_string(),
            )),
        }
    }

    fn eval_expression(&self, expr: &Expr) -> DaftResult<Series> {
        use crate::Expr::*;
        let expected_field = expr.to_field(self.schema.as_ref())?;
        let series = match expr {
            Alias(child, name) => Ok(self.eval_expression(child)?.rename(name)),
            Agg(agg_expr) => self.eval_agg_expression(agg_expr, None),
            Cast(child, dtype) => self.eval_expression(child)?.cast(dtype),
            Column(name) => self.get_column(name).cloned(),
            Not(child) => !(self.eval_expression(child)?),
            IsNull(child) => self.eval_expression(child)?.is_null(),
            NotNull(child) => self.eval_expression(child)?.not_null(),
            FillNull(child, fill_value) => {
                let fill_value = self.eval_expression(fill_value)?;
                self.eval_expression(child)?.fill_null(&fill_value)
            }
            IsIn(child, items) => self
                .eval_expression(child)?
                .is_in(&self.eval_expression(items)?),
            Between(child, lower, upper) => self
                .eval_expression(child)?
                .between(&self.eval_expression(lower)?, &self.eval_expression(upper)?),
            BinaryOp { op, left, right } => {
                let lhs = self.eval_expression(left)?;
                let rhs = self.eval_expression(right)?;
                use daft_core::array::ops::{DaftCompare, DaftLogical};
                use daft_dsl::Operator::*;
                match op {
                    Plus => lhs + rhs,
                    Minus => lhs - rhs,
                    TrueDivide => lhs / rhs,
                    Multiply => lhs * rhs,
                    Modulus => lhs % rhs,
                    Lt => Ok(lhs.lt(&rhs)?.into_series()),
                    LtEq => Ok(lhs.lte(&rhs)?.into_series()),
                    Eq => Ok(lhs.equal(&rhs)?.into_series()),
                    NotEq => Ok(lhs.not_equal(&rhs)?.into_series()),
                    GtEq => Ok(lhs.gte(&rhs)?.into_series()),
                    Gt => Ok(lhs.gt(&rhs)?.into_series()),
                    And => lhs.and(&rhs),
                    Or => lhs.or(&rhs),
                    Xor => lhs.xor(&rhs),
                    ShiftLeft => lhs.shift_left(&rhs),
                    ShiftRight => lhs.shift_right(&rhs),
                    _ => panic!("{op:?} not supported"),
                }
            }
            Function { func, inputs } => {
                let evaluated_inputs = inputs
                    .iter()
                    .map(|e| self.eval_expression(e))
                    .collect::<DaftResult<Vec<_>>>()?;
                func.evaluate(evaluated_inputs.as_slice(), func)
            }
            ScalarFunction(func) => {
                let evaluated_inputs = func
                    .inputs
                    .iter()
                    .map(|e| self.eval_expression(e))
                    .collect::<DaftResult<Vec<_>>>()?;
                func.udf.evaluate(evaluated_inputs.as_slice())
            }
            Literal(lit_value) => Ok(lit_value.to_series()),
            IfElse {
                if_true,
                if_false,
                predicate,
            } => match predicate.as_ref() {
                Expr::Literal(LiteralValue::Boolean(true)) => self.eval_expression(if_true),
                Expr::Literal(LiteralValue::Boolean(false)) => {
                    Ok(self.eval_expression(if_false)?.rename(if_true.name()))
                }
                _ => {
                    let if_true_series = self.eval_expression(if_true)?;
                    let if_false_series = self.eval_expression(if_false)?;
                    let predicate_series = self.eval_expression(predicate)?;
                    Ok(if_true_series.if_else(&if_false_series, &predicate_series)?)
                }
            },
        }?;
        if expected_field.name != series.field().name {
            return Err(DaftError::ComputeError(format!(
                "Mismatch of expected expression name and name from computed series ({} vs {}) for expression: {expr}",
                expected_field.name,
                series.field().name
            )));
        }
        if expected_field.dtype != series.field().dtype {
            panic!("Mismatch of expected expression data type and data type from computed series, {} vs {}", expected_field.dtype, series.field().dtype);
        }
        Ok(series)
    }

    pub fn eval_expression_list(&self, exprs: &[ExprRef]) -> DaftResult<Self> {
        let result_series = exprs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<DaftResult<Vec<Series>>>()?;

        let fields = result_series
            .iter()
            .map(|s| s.field().clone())
            .collect::<Vec<Field>>();
        let mut seen: HashSet<String> = HashSet::new();
        for field in fields.iter() {
            let name = &field.name;
            if seen.contains(name) {
                return Err(DaftError::ValueError(format!(
                    "Duplicate name found when evaluating expressions: {name}"
                )));
            }
            seen.insert(name.clone());
        }
        let new_schema = Schema::new(fields)?;

        let has_agg_expr = exprs.iter().any(|e| matches!(e.as_ref(), Expr::Agg(..)));
        let num_rows = match (has_agg_expr, self.len()) {
            // "Normal" case: the final cardinality is the max(*results_lens, self.len())
            // This correctly accounts for broadcasting of literals, which can have unit length
            (false, self_len) if self_len > 0 => result_series
                .iter()
                .map(|s| s.len())
                .chain(std::iter::once(self.len()))
                .max()
                .unwrap(),
            // "Empty" case: when no aggregation is applied, the expected result should also be empty
            (false, _) => 0,
            // "Aggregation" case: the final cardinality is the max(results' lens)
            // We discard the original self.len() because we expect aggregations to change
            // the final cardinality. Aggregations on empty tables are expected to produce unit length results.
            (true, _) => result_series.iter().map(|s| s.len()).max().unwrap(),
        };

        Table::new_with_broadcast(new_schema, result_series, num_rows)
    }

    pub fn as_physical(&self) -> DaftResult<Self> {
        let new_series: Vec<Series> = self
            .columns
            .iter()
            .map(|s| s.as_physical())
            .collect::<DaftResult<Vec<_>>>()?;
        let new_schema = Schema::new(new_series.iter().map(|s| s.field().clone()).collect())?;
        Table::new_with_size(new_schema, new_series, self.len())
    }

    pub fn cast_to_schema(&self, schema: &Schema) -> DaftResult<Self> {
        self.cast_to_schema_with_fill(schema, None)
    }

    pub fn cast_to_schema_with_fill(
        &self,
        schema: &Schema,
        fill_map: Option<&HashMap<&str, ExprRef>>,
    ) -> DaftResult<Self> {
        let current_col_names = HashSet::<_>::from_iter(self.column_names());
        let null_lit = null_lit();
        let exprs: Vec<_> = schema
            .fields
            .iter()
            .map(|(name, field)| {
                if current_col_names.contains(name) {
                    // For any fields already in the table, perform a cast
                    col(name.clone()).cast(&field.dtype)
                } else {
                    // For any fields in schema that are not in self.schema, use fill map to fill with an expression.
                    // If no entry for column name, fall back to null literal (i.e. create a null array for that column).
                    fill_map
                        .as_ref()
                        .and_then(|m| m.get(name.as_str()))
                        .unwrap_or(&null_lit)
                        .clone()
                        .alias(name.clone())
                        .cast(&field.dtype)
                }
            })
            .collect();
        self.eval_expression_list(&exprs)
    }

    pub fn repr_html(&self) -> String {
        // Produces a <table> HTML element.

        let mut res = "<table class=\"dataframe\">\n".to_string();

        // Begin the header.
        res.push_str("<thead><tr>");

        for (name, field) in &self.schema.fields {
            res.push_str(
                "<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left\">",
            );
            res.push_str(&html_escape::encode_text(name));
            res.push_str("<br />");
            res.push_str(&html_escape::encode_text(&format!("{}", field.dtype)));
            res.push_str("</th>");
        }

        // End the header.
        res.push_str("</tr></thead>\n");

        // Begin the body.
        res.push_str("<tbody>\n");

        let head_rows;
        let tail_rows;

        if self.len() > 10 {
            head_rows = 5;
            tail_rows = 5;
        } else {
            head_rows = self.len();
            tail_rows = 0;
        }

        let styled_td =
            "<td><div style=\"text-align:left; max-width:192px; max-height:64px; overflow:auto\">";

        for i in 0..head_rows {
            // Begin row.
            res.push_str("<tr>");

            for col in self.columns.iter() {
                res.push_str(styled_td);
                res.push_str(&col.html_value(i));
                res.push_str("</div></td>");
            }

            // End row.
            res.push_str("</tr>\n");
        }

        if tail_rows != 0 {
            res.push_str("<tr>");
            for _ in self.columns.iter() {
                res.push_str("<td>...</td>");
            }
            res.push_str("</tr>\n");
        }

        for i in (self.len() - tail_rows)..(self.len()) {
            // Begin row.
            res.push_str("<tr>");

            for col in self.columns.iter() {
                res.push_str(styled_td);
                res.push_str(&col.html_value(i));
                res.push_str("</td>");
            }

            // End row.
            res.push_str("</tr>\n");
        }

        // End the body and the table.
        res.push_str("</tbody>\n</table>");

        res
    }

    pub fn to_comfy_table(&self, max_col_width: Option<usize>) -> comfy_table::Table {
        make_comfy_table(
            self.schema
                .fields
                .values()
                .map(Cow::Borrowed)
                .collect::<Vec<_>>()
                .as_slice(),
            Some(self.columns.iter().collect::<Vec<_>>().as_slice()),
            max_col_width,
        )
    }
}

impl Display for Table {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = self.to_comfy_table(Some(32));
        writeln!(f, "{table}")
    }
}

impl AsRef<Table> for Table {
    fn as_ref(&self) -> &Table {
        self
    }
}

#[cfg(test)]
mod test {

    use crate::Table;
    use common_error::DaftResult;
    use daft_core::datatypes::{DataType, Float64Array, Int64Array};
    use daft_core::schema::Schema;
    use daft_core::series::IntoSeries;
    use daft_dsl::col;
    #[test]
    fn add_int_and_float_expression() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3])).into_series();
        let b = Float64Array::from(("b", vec![1., 2., 3.])).into_series();
        let schema = Schema::new(vec![
            a.field().clone().rename("a"),
            b.field().clone().rename("b"),
        ])?;
        let table = Table::from_nonempty_columns(vec![a, b])?;
        let e1 = col("a").add(col("b"));
        let result = table.eval_expression(&e1)?;
        assert_eq!(*result.data_type(), DataType::Float64);
        assert_eq!(result.len(), 3);

        let e2 = col("a").add(col("b")).cast(&DataType::Int64);
        let result = table.eval_expression(&e2)?;
        assert_eq!(*result.data_type(), DataType::Int64);
        assert_eq!(result.len(), 3);

        Ok(())
    }
}
