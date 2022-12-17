use crate::{
    datatypes::DataType, dsl::lit, error::DaftError, error::DaftResult, field::Field,
    schema::Schema,
};
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
    pub fn to_field(&self, schema: &Schema) -> DaftResult<Field> {
        use Expr::*;

        match self {
            Alias(expr, name) => Ok(Field::new(name, expr.get_type(schema))),
            Column(name) => Ok(schema.get_field(name)?.clone()),
            _ => Err(DaftError::NotFound("no found".into())),
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
