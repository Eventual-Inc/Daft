use common_error::DaftResult;
use common_treenode::Transformed;
use daft_core::lit::Literal;
use daft_dsl::{Expr, ExprRef, Operator, lit, null_lit};
use daft_schema::schema::SchemaRef;

fn is_null(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Literal::Null))
}

/// Simplify expressions with null
pub(crate) fn simplify_expr_with_null(
    expr: ExprRef,
    _schema: &SchemaRef,
) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::BinaryOp { op, left, right } if is_null(left) || is_null(right) => match op {
            // ops where one null input always yields null
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::TrueDivide
            | Operator::FloorDivide
            | Operator::Modulus
            | Operator::Xor
            | Operator::ShiftLeft
            | Operator::ShiftRight => Transformed::yes(null_lit()),

            // operators where null input may not result in null
            Operator::EqNullSafe | Operator::And | Operator::Or => Transformed::no(expr),
        },
        // !NULL -> NULL
        Expr::Not(e) if is_null(e) => Transformed::yes(null_lit()),
        Expr::IsNull(e) => {
            match e.as_ref() {
                Expr::Literal(l) => {
                    match l {
                        // is_null(NULL) -> true
                        Literal::Null => Transformed::yes(lit(true)),
                        // is_null(lit(x)) -> false
                        _ => Transformed::yes(lit(false)),
                    }
                }
                _ => Transformed::no(expr),
            }
        }
        Expr::NotNull(e) => {
            match e.as_ref() {
                Expr::Literal(l) => {
                    match l {
                        // not_null(NULL) -> false
                        Literal::Null => Transformed::yes(lit(false)),
                        // not_null(lit(x)) -> true
                        _ => Transformed::yes(lit(true)),
                    }
                }
                _ => Transformed::no(expr),
            }
        }
        // if NULL { a } else { b } -> NULL
        Expr::IfElse { predicate, .. } if is_null(predicate) => Transformed::yes(null_lit()),
        // NULL IN (...) -> NULL
        Expr::IsIn(e, _) if is_null(e) => Transformed::yes(null_lit()),
        _ => Transformed::no(expr),
    })
}
