use common_error::DaftResult;
use common_treenode::Transformed;
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
        Expr::Literal(LiteralValue::Int8(1))
        | Expr::Literal(LiteralValue::UInt8(1))
        | Expr::Literal(LiteralValue::Int16(1))
        | Expr::Literal(LiteralValue::UInt16(1))
        | Expr::Literal(LiteralValue::Int32(1))
        | Expr::Literal(LiteralValue::UInt32(1))
        | Expr::Literal(LiteralValue::Int64(1))
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
        Expr::Literal(LiteralValue::Int8(0))
        | Expr::Literal(LiteralValue::UInt8(0))
        | Expr::Literal(LiteralValue::Int16(0))
        | Expr::Literal(LiteralValue::UInt16(0))
        | Expr::Literal(LiteralValue::Int32(0))
        | Expr::Literal(LiteralValue::UInt32(0))
        | Expr::Literal(LiteralValue::Int64(0))
        | Expr::Literal(LiteralValue::UInt64(0))
        | Expr::Literal(LiteralValue::Float64(0.)) => true,
        Expr::Literal(LiteralValue::Decimal(v, _p, _s)) if *v == 0 => true,
        _ => false,
    }
}

// simplify expressions with numeric operators
pub(crate) fn simplify_numeric_expr(
    expr: ExprRef,
    _schema: &SchemaRef,
) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::BinaryOp { op, left, right } => {
            match op {
                // TODO: Can't do this one because we don't have a way to determine if an expr potentially contains nulls (nullable)
                // A * 0 --> 0 (if A is not null and not floating/decimal)
                // 0 * A --> 0 (if A is not null and not floating/decimal)

                // A * 1 -> A
                // 1 * A -> A
                Operator::Multiply if is_one(left) => Transformed::yes(right.clone()),
                Operator::Multiply if is_one(right) => Transformed::yes(left.clone()),
                // A / 1 -> A
                Operator::TrueDivide if is_one(right) => Transformed::yes(left.clone()),
                // A + 0 -> A
                // 0 + A -> A
                Operator::Plus if is_zero(left) => Transformed::yes(right.clone()),
                Operator::Plus if is_zero(right) => Transformed::yes(left.clone()),
                // A - 0 -> A
                Operator::Minus if is_zero(right) => Transformed::yes(left.clone()),

                _ => Transformed::no(expr),
            }
        }
        _ => Transformed::no(expr),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_treenode::Transformed;
    use daft_dsl::{lit, Column, Expr, ExprRef, Operator, ResolvedColumn};
    use daft_schema::schema::Schema;

    use crate::simplify::numeric::simplify_numeric_expr;

    // Helper function to create a column reference.
    fn col(name: String) -> ExprRef {
        Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::Basic(
            Arc::<str>::from(name),
        ))))
    }

    // Helper functions for tests.
    fn add(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::Plus,
            left,
            right,
        })
    }

    fn mul(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::Multiply,
            left,
            right,
        })
    }

    fn div(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::TrueDivide,
            left,
            right,
        })
    }

    fn sub(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::Minus,
            left,
            right,
        })
    }

    #[test]
    fn test_simplify_numeric_identity_operations() -> DaftResult<()> {
        let empty_schema = Arc::new(Schema::empty());
        let col_expr = col("a".to_string());

        // Test addition with 0.
        let expr = add(col_expr.clone(), lit(0));
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        let expr = add(lit(0), col_expr.clone());
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        // Test multiplication with 1.
        let expr = mul(col_expr.clone(), lit(1));
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        let expr = mul(lit(1), col_expr.clone());
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        // Test division by 1.
        let expr = div(col_expr.clone(), lit(1));
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        // Test subtraction with 0.
        let expr = sub(col_expr.clone(), lit(0));
        let simplified = simplify_numeric_expr(expr, &empty_schema)?;
        assert_eq!(simplified, Transformed::yes(col_expr.clone()));

        Ok(())
    }

    #[test]
    fn test_simplify_numeric_identity_operations_edge_cases() -> DaftResult<()> {
        let empty_schema = Arc::new(Schema::empty());
        let col_expr = col("a".to_string());

        // 1 / a should not be simplified.
        let expr = div(lit(1), col_expr.clone());
        let simplified = simplify_numeric_expr(expr.clone(), &empty_schema)?;
        assert_eq!(simplified, Transformed::no(expr));

        // 0 - a should not be simplified.
        let expr = sub(lit(0), col_expr.clone());
        let simplified = simplify_numeric_expr(expr.clone(), &empty_schema)?;
        assert_eq!(simplified, Transformed::no(expr));

        Ok(())
    }
}
