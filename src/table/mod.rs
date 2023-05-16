use std::fmt::{Display, Formatter, Result};

use num_traits::ToPrimitive;

use crate::array::ops::GroupIndices;

use crate::datatypes::logical::LogicalArray;
use crate::datatypes::{BooleanType, DataType, Field, UInt64Array};
use crate::dsl::functions::FunctionEvaluator;
use crate::dsl::{AggExpr, Expr};
use crate::error::{DaftError, DaftResult};
use crate::schema::{Schema, SchemaRef};
use crate::series::{IntoSeries, Series};
use crate::{with_match_daft_logical_types, with_match_physical_daft_types};
mod ops;
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
                    if field.dtype.is_logical() {
                        with_match_daft_logical_types!(field.dtype, |$T| {
                            columns.push(LogicalArray::<$T>::empty(field_name, &field.dtype).into_series())
                        })
                    } else {
                        with_match_physical_daft_types!(field.dtype, |$T| {
                            columns.push(DataArray::<$T>::empty(field_name, &field.dtype).into_series())
                        })
                    }
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

    pub fn head(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            return Ok(Table {
                schema: self.schema.clone(),
                columns: self.columns.clone(),
            });
        }

        let new_series: DaftResult<Vec<_>> = self.columns.iter().map(|s| s.head(num)).collect();
        Ok(Table {
            schema: self.schema.clone(),
            columns: new_series?,
        })
    }

    pub fn sample(&self, num: usize) -> DaftResult<Self> {
        if num >= self.len() {
            Ok(self.clone())
        } else {
            use rand::{distributions::Uniform, Rng};
            let range = Uniform::from(0..self.len() as u64);
            let values: Vec<u64> = rand::thread_rng().sample_iter(&range).take(num).collect();
            let indices: crate::array::DataArray<crate::datatypes::UInt64Type> =
                UInt64Array::from(("idx", values));
            self.take(&indices.into_series())
        }
    }

    pub fn quantiles(&self, num: usize) -> DaftResult<Self> {
        if self.len() == 0 {
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

        let mask = mask.downcast::<BooleanType>().unwrap();
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
        use crate::dsl::AggExpr::*;
        match agg_expr {
            Count(expr) => Series::count(&self.eval_expression(expr)?, groups),
            Sum(expr) => Series::sum(&self.eval_expression(expr)?, groups),
            Mean(expr) => Series::mean(&self.eval_expression(expr)?, groups),
            Min(expr) => Series::min(&self.eval_expression(expr)?, groups),
            Max(expr) => Series::max(&self.eval_expression(expr)?, groups),
            List(expr) => Series::agg_list(&self.eval_expression(expr)?, groups),
            Concat(expr) => Series::agg_concat(&self.eval_expression(expr)?, groups),
        }
    }

    fn eval_expression(&self, expr: &Expr) -> DaftResult<Series> {
        use crate::dsl::Expr::*;
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
                use crate::array::ops::{DaftCompare, DaftLogical};
                use crate::dsl::Operator::*;
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
                func.evaluate(evaluated_inputs.as_slice())
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
        use std::collections::HashSet;
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
}

impl Display for Table {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
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
                .map(|s| s.str_value(i))
                .collect::<DaftResult<Vec<String>>>()
                .unwrap();
            table.add_row(row.into());
        }
        if tail_rows != 0 {
            let row: prettytable::Row = (0..self.num_columns()).map(|_| "...").collect();
            table.add_row(row);
        }

        for i in 0..tail_rows {
            let row = self
                .columns
                .iter()
                .map(|s| s.str_value(self.len() - tail_rows - 1 + i))
                .collect::<DaftResult<Vec<String>>>()
                .unwrap();
            table.add_row(row.into());
        }

        write!(f, "{table}")
    }
}

#[cfg(test)]
mod test {

    use crate::datatypes::{DataType, Float64Array, Int64Array};
    use crate::dsl::col;
    use crate::error::DaftResult;
    use crate::schema::Schema;
    use crate::series::IntoSeries;
    use crate::table::Table;
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
