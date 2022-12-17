use crate::{datatypes::DataType, dsl::lit, field::Field, schema::Schema};
use std::{fmt::Debug, sync::Arc};

type ExprRef = Arc<Expr>;

#[derive(Debug)]
pub enum Expr {
    Alias(ExprRef, Arc<str>),
    BinaryOp {
        op: Arc<str>,
        left: ExprRef,
        right: ExprRef,
    },

    Column(Arc<str>),
    Literal(lit::LiteralValue),
}

impl Expr {
    pub fn to_field(&self, schema: &Schema) -> Field {
        use Expr::*;

        match self {
            Alias(expr, name) => Field::new(name, expr.get_type(schema)),
            _ => Field::new("x", self.get_type(schema)),
        }
    }
    pub fn get_type(&self, schema: &Schema) -> DataType {
        DataType::Unknown
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl::lit::LiteralValue;

    use super::*;

    #[test]
    fn it_adds_two() {
        let x = Expr::Literal(LiteralValue::Int64(10));
        let y = Expr::Literal(LiteralValue::Int64(12));
        let z = Expr::BinaryOp {
            left: x.into(),
            right: y.into(),
            op: "+".into(),
        };

        println!("hello: {:?}", z);
    }
}
