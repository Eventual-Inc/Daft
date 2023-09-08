#![feature(hash_raw_entry)]

use std::collections::HashSet;
use std::fmt::{Display, Formatter, Result};

use daft_core::array::ops::full::FullNull;
use num_traits::ToPrimitive;

use daft_core::array::ops::GroupIndices;

use common_error::{DaftError, DaftResult};
use daft_core::datatypes::{BooleanArray, DataType, Field, UInt64Array};
use daft_core::schema::{Schema, SchemaRef};
use daft_core::series::{IntoSeries, Series};

use daft_dsl::functions::FunctionEvaluator;
use daft_dsl::{col, null_lit, AggExpr, Expr};
#[cfg(feature = "python")]
mod ffi;
mod ops;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
pub use python::register_modules;

#[derive(Clone)]
pub struct Table {
    pub schema: SchemaRef,
    columns: Vec<Series>,
}

impl Table {
    pub fn new<S: Into<SchemaRef>>(schema: S, columns: Vec<Series>) -> DaftResult<Self> {
        let schema: SchemaRef = schema.into();
        if schema.fields.len() != columns.len() {
            return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the number of fields did not match between the schema and the input columns.\n {:?}\n vs\n {:?}", schema.fields.len(), columns.len())));
        }
        let mut num_rows = 1;

        for (field, series) in schema.fields.values().zip(columns.iter()) {
            if field != series.field() {
                return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the Schema Field and the Series Field  did not match. schema field: {field} vs series field: {}", series.field())));
            }
            if (series.len() != 1) && (series.len() != num_rows) {
                if num_rows == 1 {
                    num_rows = series.len();
                } else {
                    return Err(DaftError::ValueError(format!("While building a Table, we found that the Series lengths did not match. Series named: {} had length: {} vs rest of the DataFrame had length: {}", field.name, series.len(), num_rows)));
                }
            }
        }

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

        Ok(Table {
            schema,
            columns: columns?,
        })
    }

    pub fn empty(schema: Option<SchemaRef>) -> DaftResult<Self> {
        match schema {
            Some(schema) => {
                let mut columns: Vec<Series> = Vec::with_capacity(schema.names().len());
                for (field_name, field) in schema.fields.iter() {
                    let series = Series::empty(field_name, &field.dtype);
                    columns.push(series)
                }
                Ok(Table { schema, columns })
            }
            None => Self::new(Schema::empty(), vec![]),
        }
    }

    pub fn from_columns(columns: Vec<Series>) -> DaftResult<Self> {
        let fields = columns.iter().map(|s| s.field().clone()).collect();
        let schema = Schema::new(fields)?;
        Table::new(schema, columns)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema.names()
    }

    pub fn len(&self) -> usize {
        if self.num_columns() == 0 {
            0
        } else {
            self.get_column_by_index(0).unwrap().len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> =
            self.columns.iter().map(|s| s.slice(start, end)).collect();
        Ok(Table {
            schema: self.schema.clone(),
            columns: new_series?,
        })
    }

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            return Ok(Table {
                schema: self.schema.clone(),
                columns: self.columns.clone(),
            });
        }
        self.slice(0, num)
    }

    pub fn sample(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            Ok(self.clone())
        } else {
            use rand::{distributions::Uniform, Rng};
            let range = Uniform::from(0..self.len() as u64);
            let values: Vec<u64> = rand::thread_rng().sample_iter(&range).take(num).collect();
            let indices: daft_core::array::DataArray<daft_core::datatypes::UInt64Type> =
                UInt64Array::from(("idx", values));
            self.take(&indices.into_series())
        }
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

    pub fn filter(&self, predicate: &[Expr]) -> DaftResult<Self> {
        if predicate.is_empty() {
            Ok(self.clone())
        } else if predicate.len() == 1 {
            let mask = self.eval_expression(predicate.get(0).unwrap())?;
            self.mask_filter(&mask)
        } else {
            let mut expr = predicate.get(0).unwrap().and(predicate.get(1).unwrap());
            for i in 2..predicate.len() {
                let next = predicate.get(i).unwrap();
                expr = expr.and(next);
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
        Ok(Table {
            schema: self.schema.clone(),
            columns: new_series?,
        })
    }

    pub fn take(&self, idx: &Series) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.take(idx)).collect();
        Ok(Table::new(self.schema.clone(), new_series?).unwrap())
    }

    pub fn concat(tables: &[&Table]) -> DaftResult<Self> {
        if tables.is_empty() {
            return Err(DaftError::ValueError(
                "Need at least 1 table to perform concat".to_string(),
            ));
        }
        if tables.len() == 1 {
            return Ok((*tables.first().unwrap()).clone());
        }
        let first_table = tables.first().unwrap();

        let first_schema = first_table.schema.as_ref();
        for tab in tables.iter().skip(1) {
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
                .map(|s| s.get_column_by_index(i).unwrap())
                .collect();
            new_series.push(Series::concat(series_to_cat.as_slice())?);
        }
        Ok(Table {
            schema: first_table.schema.clone(),
            columns: new_series,
        })
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
        Self::from_columns(series_by_name)
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
            Mean(expr) => Series::mean(&self.eval_expression(expr)?, groups),
            Min(expr) => Series::min(&self.eval_expression(expr)?, groups),
            Max(expr) => Series::max(&self.eval_expression(expr)?, groups),
            List(expr) => Series::agg_list(&self.eval_expression(expr)?, groups),
            Concat(expr) => Series::agg_concat(&self.eval_expression(expr)?, groups),
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
                    And => Ok(lhs.and(&rhs)?.into_series()),
                    Or => Ok(lhs.or(&rhs)?.into_series()),
                    Xor => Ok(lhs.xor(&rhs)?.into_series()),
                    _ => panic!("{op:?} not supported"),
                }
            }
            Function { func, inputs } => {
                let evaluated_inputs = inputs
                    .iter()
                    .map(|e| self.eval_expression(e))
                    .collect::<DaftResult<Vec<_>>>()?;
                func.evaluate(evaluated_inputs.as_slice(), expr)
            }
            Literal(lit_value) => Ok(lit_value.to_series()),
            IfElse {
                if_true,
                if_false,
                predicate,
            } => {
                let if_true_series = self.eval_expression(if_true)?;
                let if_false_series = self.eval_expression(if_false)?;
                let predicate_series = self.eval_expression(predicate)?;
                Ok(if_true_series.if_else(&if_false_series, &predicate_series)?)
            }
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

    pub fn eval_expression_list(&self, exprs: &[Expr]) -> DaftResult<Self> {
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
        let schema = Schema::new(fields)?;
        Table::new(schema, result_series)
    }
    pub fn as_physical(&self) -> DaftResult<Self> {
        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.as_physical()).collect();
        Table::from_columns(new_series?)
    }

    pub fn cast_to_schema(&self, schema: &Schema) -> DaftResult<Self> {
        let current_col_names = HashSet::<_>::from_iter(self.column_names());
        let exprs: Vec<_> = schema
            .fields
            .iter()
            .map(|(name, field)| {
                if current_col_names.contains(name) {
                    // For any fields already in the table, perform a cast
                    col(name.clone()).cast(&field.dtype)
                } else {
                    // For any fields in schema that are not in self.schema, create all-null arrays
                    null_lit().alias(name.clone()).cast(&field.dtype)
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
            res.push_str("<th style=\"text-wrap: nowrap; max-width:192px; overflow:auto\">");
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

    pub fn to_prettytable(&self, max_col_width: Option<usize>) -> prettytable::Table {
        let mut table = prettytable::Table::new();
        let header = self
            .schema
            .fields
            .iter()
            .map(|(name, field)| {
                prettytable::Cell::new(format!("{}\n{}", name, field.dtype).as_str())
                    .with_style(prettytable::Attr::Bold)
            })
            .collect();
        table.add_row(header);

        let head_rows;
        let tail_rows;

        if self.len() > 10 {
            head_rows = 5;
            tail_rows = 5;
        } else {
            head_rows = self.len();
            tail_rows = 0;
        }

        for i in 0..head_rows {
            let row = self
                .columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i).unwrap();
                    if let Some(max_col_width) = max_col_width {
                        if str_val.len() > max_col_width {
                            str_val = format!("{}...", &str_val[..max_col_width - 3]);
                        }
                    }
                    str_val
                })
                .collect::<Vec<String>>();
            table.add_row(row.into());
        }
        if tail_rows != 0 {
            let row: prettytable::Row = (0..self.num_columns()).map(|_| "...").collect();
            table.add_row(row);
        }

        for i in (self.len() - tail_rows)..(self.len()) {
            let row = self
                .columns
                .iter()
                .map(|s| {
                    let mut str_val = s.str_value(i).unwrap();
                    if let Some(max_col_width) = max_col_width {
                        if str_val.len() > max_col_width {
                            str_val = format!("{}...", &str_val[..max_col_width - 3]);
                        }
                    }
                    str_val
                })
                .collect::<Vec<String>>();
            table.add_row(row.into());
        }

        table
    }
}

impl Display for Table {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        let table = self.to_prettytable(Some(32));
        write!(f, "{table}")
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
        let table = Table::new(schema, vec![a, b])?;
        let e1 = col("a") + col("b");
        let result = table.eval_expression(&e1)?;
        assert_eq!(*result.data_type(), DataType::Float64);
        assert_eq!(result.len(), 3);

        let e2 = (col("a") + col("b")).cast(&DataType::Int64);
        let result = table.eval_expression(&e2)?;
        assert_eq!(*result.data_type(), DataType::Int64);
        assert_eq!(result.len(), 3);

        Ok(())
    }
}
