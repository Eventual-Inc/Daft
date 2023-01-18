use crate::{
    datatypes::DataType, datatypes::Field, dsl::lit, error::DaftResult, schema::Schema,
    utils::supertype::try_get_supertype,
};
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

type ExprRef = Arc<Expr>;

#[derive(Debug, Clone)]
pub enum Expr {
    Alias(ExprRef, Arc<str>),
    BinaryOp {
        op: Operator,
        left: ExprRef,
        right: ExprRef,
    },

    Column(Arc<str>),
    Literal(lit::LiteralValue),
}

pub fn col(name: &str) -> Expr {
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
    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;

        match self {
            Alias(expr, name) => Ok(Field::new(name.as_ref(), expr.get_type(schema)?)),
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
                    _ => Field::new(
                        left.to_field(schema)?.name.as_str(),
                        try_get_supertype(&left.get_type(schema)?, &right.get_type(schema)?)?,
                    ),
                };
                Ok(result)
            }
        }
    }

    pub fn get_type(&self, schema: &Schema) -> DaftResult<DataType> {
        Ok(self.to_field(schema)?.dtype)
    }
}

/// Based on Polars first class operators: https://github.com/pola-rs/polars/blob/master/polars/polars-lazy/polars-plan/src/dsl/expr.rs
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
    Divide,
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
            Divide => "//",
            TrueDivide => "/",
            FloorDivide => "floor_div",
            Modulus => "%",
            And => "&",
            Or => "|",
            Xor => "^",
        };
        write!(f, "{}", tkn)
    }
}

impl Operator {
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
        let schema = Schema::new(&[
            ("x".to_string(), DataType::Float64),
            ("y".to_string(), DataType::Int64),
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
