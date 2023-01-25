use std::fmt::{Display, Formatter, Result};
use std::sync::Arc;

use crate::datatypes::Field;
use crate::dsl::Expr;
use crate::error::{DaftError, DaftResult};
use crate::schema::Schema;
use crate::series::Series;

pub struct Table {
    schema: Arc<Schema>,
    columns: Vec<Series>,
}

impl Table {
    pub fn new(schema: Schema, columns: Vec<Series>) -> DaftResult<Self> {
        if schema.fields.len() != columns.len() {
            return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the number of fields did not match between the schema and the input columns. {} vs {}", schema.fields.len(), columns.len())));
        }
        let mut num_rows = 1;
        for (field, series) in schema.fields.values().zip(columns.iter()) {
            if field != series.field() {
                return Err(DaftError::SchemaMismatch(format!("While building a Table, we found that the Schema Field and the Series Field  did not match. schema field: {:?} vs series field: {:?}", field, series.field())));
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
            schema: schema.into(),
            columns: columns?,
        })
    }

    pub fn from_columns(columns: Vec<Series>) -> DaftResult<Self> {
        let fields = columns.iter().map(|s| s.field().clone()).collect();
        let schema = Schema::new(fields);
        Table::new(schema, columns)
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
    pub fn len(&self) -> usize {
        if self.num_columns() == 0 {
            0
        } else {
            self.get_column_by_index(0).unwrap().len()
        }
    }
    //pub fn head(&self, num: usize) -> DaftResult<Table>;
    //pub fn sample(&self, num: usize) -> DaftResult<Table>;
    //pub fn filter(&self, predicate: &[&Expr]) -> DaftResult<Table>;
    //pub fn sort(&self, sort_keys: &[&Expr], descending: &[bool]) -> DaftResult<Table>;
    //pub fn argsort(&self, sort_keys: &[&Expr], descending: &[bool]) -> DaftResult<Series>;
    //pub fn take(&self, idx: &Series) -> DaftResult<Table>;
    //pub fn concat(tables: &[&Table]) -> DaftResult<Table>;

    pub fn get_column<S: AsRef<str>>(&self, name: S) -> DaftResult<Series> {
        let i = self.schema.get_index(name.as_ref())?;
        Ok(self.columns.get(i).unwrap().clone())
    }

    pub fn get_column_by_index(&self, idx: usize) -> DaftResult<Series> {
        Ok(self.columns.get(idx).unwrap().clone())
    }

    fn eval_expression(&self, expr: &Expr) -> DaftResult<Series> {
        use crate::dsl::Expr::*;
        match expr {
            Alias(child, _name) => self.eval_expression(child),
            Column(name) => self.get_column(name),
            BinaryOp { op, left, right } => {
                let lhs = self.eval_expression(left)?;
                let rhs = self.eval_expression(right)?;
                use crate::dsl::Operator::*;
                match op {
                    Plus => Ok(lhs + rhs),
                    Minus => Ok(lhs - rhs),
                    Divide => Ok(lhs / rhs),
                    Multiply => Ok(lhs * rhs),
                    Modulus => Ok(lhs % rhs),
                    _ => panic!("{:?} not supported", op),
                }
            }
            Literal(lit_value) => Ok(lit_value.to_series()),
        }
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
        let schema = Schema::new(fields);
        Table::new(schema, result_series)
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
                prettytable::Cell::new(format!("{}\n{:?}", name, field.dtype).as_str())
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
                .map(|s| s.str_value(self.len() - i - 1))
                .collect::<DaftResult<Vec<String>>>()
                .unwrap();
            table.add_row(row.into());
        }

        write!(f, "{}", table)
    }
}

#[cfg(test)]
mod test {

    use crate::array::BaseArray;
    use crate::datatypes::{DataType, Int64Array};
    use crate::dsl::col;
    use crate::schema::Schema;
    use crate::table::Table;
    use crate::{datatypes::Float64Array, error::DaftResult};
    #[test]
    fn add_int_and_float_expression() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice())).into_series();
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice())).into_series();
        let schema = Schema::new(vec![
            a.field().clone().rename("a"),
            b.field().clone().rename("b"),
        ]);
        let table = Table::new(schema, vec![a, b])?;
        let e1 = col("a") + col("b");
        let result = table.eval_expression(&e1)?;
        assert_eq!(*result.data_type(), DataType::Float64);
        assert_eq!(result.len(), 3);

        Ok(())
    }
}
