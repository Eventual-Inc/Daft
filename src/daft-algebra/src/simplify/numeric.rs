use common_error::DaftResult;
use common_treenode::Transformed;
use daft_core::utils::supertype::try_get_supertype;
use daft_dsl::{Expr, ExprRef, LiteralValue, Operator};
use daft_schema::schema::SchemaRef;

static POWS_OF_TEN: [i128; 38] = [
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
    10000000000000000000,
    100000000000000000000,
    1000000000000000000000,
    10000000000000000000000,
    100000000000000000000000,
    1000000000000000000000000,
    10000000000000000000000000,
    100000000000000000000000000,
    1000000000000000000000000000,
    10000000000000000000000000000,
    100000000000000000000000000000,
    1000000000000000000000000000000,
    10000000000000000000000000000000,
    100000000000000000000000000000000,
    1000000000000000000000000000000000,
    10000000000000000000000000000000000,
    100000000000000000000000000000000000,
    1000000000000000000000000000000000000,
    10000000000000000000000000000000000000,
];

fn is_one(s: &Expr) -> bool {
    match s {
        Expr::Literal(LiteralValue::Int32(1))
        | Expr::Literal(LiteralValue::Int64(1))
        | Expr::Literal(LiteralValue::UInt32(1))
        | Expr::Literal(LiteralValue::UInt64(1))
        | Expr::Literal(LiteralValue::Float64(1.)) => true,

        Expr::Literal(LiteralValue::Decimal(v, _p, s)) => {
            *s >= 0 && POWS_OF_TEN.get(*s as usize).is_some_and(|pow| v == pow)
        }
        _ => false,
    }
}

fn is_zero(s: &Expr) -> bool {
    match s {
        Expr::Literal(LiteralValue::Int32(0))
        | Expr::Literal(LiteralValue::Int64(0))
        | Expr::Literal(LiteralValue::UInt32(0))
        | Expr::Literal(LiteralValue::UInt64(0))
        | Expr::Literal(LiteralValue::Float64(0.)) => true,
        Expr::Literal(LiteralValue::Decimal(v, _p, _s)) if *v == 0 => true,
        _ => false,
    }
}

// simplify expressions with numeric operators
pub(crate) fn simplify_numeric_expr(
    expr: ExprRef,
    schema: &SchemaRef,
) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::BinaryOp { op, left, right } => {
            let supertype = try_get_supertype(&left.get_type(schema)?, &right.get_type(schema)?)?;

            match op {
                // TODO: Can't do this one because we don't have a way to determine if an expr potentially contains nulls (nullable)
                // A * 0 --> 0 (if A is not null and not floating/decimal)
                // 0 * A --> 0 (if A is not null and not floating/decimal)

                // A * 1 -> A
                Operator::Multiply if is_one(left) => {
                    Transformed::yes(right.clone().cast(&supertype))
                }
                Operator::Multiply if is_one(right) => {
                    Transformed::yes(left.clone().cast(&supertype))
                }
                // A / 1 -> A
                Operator::TrueDivide if is_one(left) => {
                    Transformed::yes(right.clone().cast(&supertype))
                }
                Operator::TrueDivide if is_one(right) => {
                    Transformed::yes(left.clone().cast(&supertype))
                }
                // A + 0 -> A
                Operator::Plus if is_zero(left) => Transformed::yes(right.clone().cast(&supertype)),
                Operator::Plus if is_zero(right) => Transformed::yes(left.clone().cast(&supertype)),
                // A - 0 -> A
                Operator::Minus if is_zero(left) => {
                    Transformed::yes(right.clone().cast(&supertype))
                }
                Operator::Minus if is_zero(right) => {
                    Transformed::yes(left.clone().cast(&supertype))
                }

                _ => Transformed::no(expr),
            }
        }
        _ => Transformed::no(expr),
    })
}
