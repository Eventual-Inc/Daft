use crate::{
    datatypes::DataType, datatypes::Field, dsl::lit, error::DaftResult, schema::Schema,
    utils::supertype::try_get_supertype,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display, Formatter, Result},
    sync::Arc,
};

type ExprRef = Arc<Expr>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Alias(ExprRef, Arc<str>),
    Agg(AggExpr),
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },
    Cast(ExprRef, DataType),
    Column(Arc<str>),
    Literal(lit::LiteralValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggExpr {
    Sum(ExprRef),
}

pub fn col<S: Into<Arc<str>>>(name: S) -> Expr {
    Expr::Column(name.into())
}

pub fn binary_op(op: Operator, left: &Expr, right: &Expr) -> Expr {
    Expr::BinaryOp {
        op,
        left: left.clone().into(),
        right: right.clone().into(),
    }
}

impl Expr {
    pub fn alias<S: Into<Arc<str>>>(&self, name: S) -> Self {
        Expr::Alias(self.clone().into(), name.into())
    }

    pub fn cast(&self, dtype: &DataType) -> Self {
        Expr::Cast(self.clone().into(), dtype.clone())
    }

    pub fn and(&self, other: &Self) -> Self {
        binary_op(Operator::And, self, other)
    }

    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use AggExpr::*;
        use Expr::*;

        match self {
            Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
            Agg(agg_expr) => match agg_expr {
                Sum(expr) => expr.to_field(schema),
            },
            Cast(expr, dtype) => Ok(Field::new(expr.name()?, dtype.clone())),
            Column(name) => Ok(schema.get_field(name).cloned()?),
            Literal(value) => Ok(Field::new("literal", value.get_type())),
            BinaryOp { op, left, right } => {
                let result = match op {
                    Operator::Lt
                    | Operator::Gt
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::And
                    | Operator::LtEq
                    | Operator::GtEq
                    | Operator::Or => {
                        Field::new(left.to_field(schema)?.name.as_str(), DataType::Boolean)
                    }
                    Operator::TrueDivide => {
                        Field::new(left.to_field(schema)?.name.as_str(), DataType::Float64)
                    }
                    _ => {
                        let left_field = left.to_field(schema)?;
                        let right_field = right.to_field(schema)?;
                        Field::new(
                            left_field.name.as_str(),
                            try_get_supertype(&left_field.dtype, &right_field.dtype)?,
                        )
                    }
                };
                Ok(result)
            }
        }
    }

    pub fn name(&self) -> DaftResult<&str> {
        use AggExpr::*;
        use Expr::*;
        match self {
            Alias(.., name) => Ok(name.as_ref()),
            Agg(agg_expr) => match agg_expr {
                Sum(expr) => expr.name(),
            },
            Cast(expr, ..) => expr.name(),
            Column(name) => Ok(name.as_ref()),
            Literal(..) => Ok("literal"),
            BinaryOp {
                op: _,
                left,
                right: _,
            } => left.name(),
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DaftResult<DataType> {
        Ok(self.to_field(schema)?.dtype)
    }
}

impl Display for Expr {
    // `f` is a buffer, and this method must write the formatted string into it
    fn fmt(&self, f: &mut Formatter) -> Result {
        use AggExpr::*;
        use Expr::*;
        match self {
            Alias(expr, name) => write!(f, "{expr} AS {name}"),
            Agg(agg_expr) => match agg_expr {
                Sum(expr) => write!(f, "sum({expr})"),
            },
            BinaryOp { op, left, right } => {
                let write_out_expr = |f: &mut Formatter, input: &Expr| match input {
                    Alias(e, _) => write!(f, "{e}"),
                    BinaryOp { .. } => write!(f, "[{input}]"),
                    _ => write!(f, "{input}"),
                };

                write_out_expr(f, left)?;
                write!(f, " {op} ")?;
                write_out_expr(f, right)?;
                Ok(())
            }
            Cast(expr, dtype) => write!(f, "cast({expr} AS {dtype})"),
            Column(name) => write!(f, "col({name})"),
            Literal(val) => write!(f, "lit({val})"),
        }
    }
}

/// Based on Polars first class operators: https://github.com/pola-rs/polars/blob/master/polars/polars-lazy/polars-plan/src/dsl/expr.rs
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    TrueDivide,
    FloorDivide,
    Modulus,
    And,
    Or,
    Xor,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Operator::*;
        let tkn = match self {
            Eq => "==",
            NotEq => "!=",
            Lt => "<",
            LtEq => "<=",
            Gt => ">",
            GtEq => ">=",
            Plus => "+",
            Minus => "-",
            Multiply => "*",
            TrueDivide => "/",
            FloorDivide => "//",
            Modulus => "%",
            And => "&",
            Or => "|",
            Xor => "^",
        };
        write!(f, "{tkn}")
    }
}

impl Operator {
    #![allow(dead_code)]
    pub(crate) fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::NotEq
                | Self::Lt
                | Self::LtEq
                | Self::Gt
                | Self::GtEq
                | Self::And
                | Self::Or
                | Self::Xor
        )
    }

    pub(crate) fn is_arithmetic(&self) -> bool {
        !(self.is_comparison())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::dsl::lit;
    #[test]
    fn check_comparision_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Lt,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Boolean);
        Ok(())
    }

    #[test]
    fn check_alias_type() -> DaftResult<()> {
        let a = col("a");
        let b = a.alias("b");
        match b {
            Expr::Alias(..) => Ok(()),
            other => Err(crate::error::DaftError::ValueError(format!(
                "expected expression to be a alias, got {other:?}"
            ))),
        }
    }

    #[test]
    fn check_arithmetic_type() -> DaftResult<()> {
        let x = lit(10.);
        let y = lit(12);
        let schema = Schema::empty();

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = lit(10.);
        let y = lit(12);

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }

    #[test]
    fn check_arithmetic_type_with_columns() -> DaftResult<()> {
        let x = col("x");
        let y = col("y");
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64),
            Field::new("y", DataType::Int64),
        ]);

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        let x = col("x");
        let y = col("y");

        let z = Expr::BinaryOp {
            left: y.into(),
            right: x.into(),
            op: Operator::Plus,
        };
        assert_eq!(z.get_type(&schema)?, DataType::Float64);

        Ok(())
    }
}
