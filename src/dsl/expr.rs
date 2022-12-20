use crate::{
    datatypes::{ArrowType, DataType},
    dsl::lit,
    error::DaftError,
    error::DaftResult,
    field::Field,
    schema::Schema,
    utils::supertype::{get_supertype, try_get_supertype},
};
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

type ExprRef = Arc<Expr>;

#[derive(Debug)]
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

impl Expr {
    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;

        match self {
            Alias(expr, name) => Ok(Field::new(name, expr.get_type(schema)?)),
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
                    | Operator::Or => Field::new(
                        left.to_field(schema)?.name.as_str(),
                        DataType::Arrow(ArrowType::Boolean),
                    ),
                    _ => Field::new(
                        left.to_field(schema)?.name.as_str(),
                        try_get_supertype(&left.get_type(schema)?, &right.get_type(schema)?)?,
                    ),
                };
                Ok(result)
            }

            _ => Err(DaftError::NotFound("no found".into())),
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
    use crate::dsl::lit::LiteralValue;

    use super::*;

    #[test]
    fn it_adds_two() {
        let x = Expr::Literal(LiteralValue::Float64(10.));
        let y = Expr::Literal(LiteralValue::Int64(12));
        let schema = Schema::new();

        println!(
            "hello: {:?}, {:?}",
            &x.get_type(&schema),
            &y.get_type(&schema)
        );

        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: Operator::Plus,
        };

        println!("hello: {:?}", z.to_field(&schema));
    }
}
