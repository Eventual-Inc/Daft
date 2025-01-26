use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::Transformed;
use daft_dsl::{lit, null_lit, Expr, ExprRef, LiteralValue, Operator};
use daft_schema::{dtype::DataType, schema::SchemaRef};

pub fn simplify_expr(expr: Expr, schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr {
        // ----------------
        // Eq
        // ----------------
        // true = A  --> A
        // false = A --> !A
        Expr::BinaryOp {
            op: Operator::Eq,
            left,
            right,
        }
        // A = true --> A
        // A = false --> !A
        | Expr::BinaryOp {
            op: Operator::Eq,
            left: right,
            right: left,
        } if is_bool_lit(&left) && is_bool_type(&right, schema) => {
            Transformed::yes(match as_bool_lit(&left) {
                Some(true) => right,
                Some(false) => right.not(),
                None => unreachable!(),
            })
        }

        // null = A --> null
        // A = null --> null
        Expr::BinaryOp {
            op: Operator::Eq,
            left,
            right,
        }
        | Expr::BinaryOp {
            op: Operator::Eq,
            left: right,
            right: left,
        } if is_null(&left) && is_bool_type(&right, schema) => Transformed::yes(null_lit()),

        // ----------------
        // Neq
        // ----------------
        // true != A  --> !A
        // false != A --> A
        Expr::BinaryOp {
            op: Operator::NotEq,
            left,
            right,
        }
        // A != true --> !A
        // A != false --> A
        | Expr::BinaryOp {
            op: Operator::NotEq,
            left: right,
            right: left,
        } if is_bool_lit(&left) && is_bool_type(&right, schema) => {
            Transformed::yes(match as_bool_lit(&left) {
                Some(true) => right.not(),
                Some(false) => right,
                None => unreachable!(),
            })
        }

        // null != A --> null
        // A != null --> null
        Expr::BinaryOp {
            op: Operator::NotEq,
            left,
            right,
        }
        | Expr::BinaryOp {
            op: Operator::NotEq,
            left: right,
            right: left,
        } if is_null(&left) && is_bool_type(&right, schema) => Transformed::yes(null_lit()),

        // ----------------
        // OR
        // ----------------

        // true OR A  --> true
        Expr::BinaryOp {
            op: Operator::Or,
            left,
            right: _,
        } if is_true(&left) => Transformed::yes(left),
        // false OR A  --> A
        Expr::BinaryOp {
            op: Operator::Or,
            left,
            right,
        } if is_false(&left) => Transformed::yes(right),
        // A OR true  --> true
        Expr::BinaryOp {
            op: Operator::Or,
            left: _,
            right,
        } if is_true(&right) => Transformed::yes(right),
        // A OR false --> A
        Expr::BinaryOp {
            left,
            op: Operator::Or,
            right,
        } if is_false(&right) => Transformed::yes(left),

        // ----------------
        // AND (TODO)
        // ----------------

        // ----------------
        // Multiplication
        // ----------------

        // A * 1 --> A
        // 1 * A --> A
        Expr::BinaryOp {
            op: Operator::Multiply,
            left,
            right,
        }| Expr::BinaryOp {
            op: Operator::Multiply,
            left: right,
            right: left,
        } if is_one(&right) => Transformed::yes(left),

        // A * null --> null
        Expr::BinaryOp {
            op: Operator::Multiply,
            left: _,
            right,
        } if is_null(&right) => Transformed::yes(right),
        // null * A --> null
        Expr::BinaryOp {
            op: Operator::Multiply,
            left,
            right: _,
        } if is_null(&left) => Transformed::yes(left),

        // TODO: Can't do this one because we don't have a way to determine if an expr potentially contains nulls (nullable)
        // A * 0 --> 0 (if A is not null and not floating/decimal)
        // 0 * A --> 0 (if A is not null and not floating/decimal)

        // ----------------
        // Division
        // ----------------
        // A / 1 --> A
        Expr::BinaryOp {
            op: Operator::TrueDivide,
            left,
            right,
        } if is_one(&right) => Transformed::yes(left),
        // null / A --> null
        Expr::BinaryOp {
            op: Operator::TrueDivide,
            left,
            right: _,
        } if is_null(&left) => Transformed::yes(left),
        // A / null --> null
        Expr::BinaryOp {
            op: Operator::TrueDivide,
            left: _,
            right,
        } if is_null(&right) => Transformed::yes(right),

        // ----------------
        // Addition
        // ----------------
        // A + 0 --> A
        Expr::BinaryOp {
            op: Operator::Plus,
            left,
            right,
        } if is_zero(&right) => Transformed::yes(left),

        // 0 + A --> A
        Expr::BinaryOp {
            op: Operator::Plus,
            left,
            right,
        } if is_zero(&left) => Transformed::yes(right),

        // ----------------
        // Subtraction
        // ----------------

        // A - 0 --> A
        Expr::BinaryOp {
            op: Operator::Minus,
            left,
            right,
        } if is_zero(&right) => Transformed::yes(left),

        // A - null --> null
        Expr::BinaryOp {
            op: Operator::Minus,
            left: _,
            right,
        } if is_null(&right) => Transformed::yes(right),
        // null - A --> null
        Expr::BinaryOp {
            op: Operator::Minus,
            left,
            right: _,
        } if is_null(&left) => Transformed::yes(left),

        // ----------------
        // Modulus
        // ----------------

        // A % null --> null
        Expr::BinaryOp {
            op: Operator::Modulus,
            left: _,
            right,
        } if is_null(&right) => Transformed::yes(right),

        // null % A --> null
        Expr::BinaryOp {
            op: Operator::Modulus,
            left,
            right: _,
        } if is_null(&left) => Transformed::yes(left),

        // A BETWEEN low AND high --> A >= low AND A <= high
        Expr::Between(expr, low, high) => {
            Transformed::yes(expr.clone().lt_eq(high).and(expr.gt_eq(low)))
        }
        Expr::Not(expr) => match Arc::unwrap_or_clone(expr) {
            // NOT (BETWEEN A AND B) --> A < low OR A > high
            Expr::Between(expr, low, high) => {
                Transformed::yes(expr.clone().lt(low).or(expr.gt(high)))
            }
            // expr NOT IN () --> true
            Expr::IsIn(_, list) if list.is_empty() => Transformed::yes(lit(true)),

            expr => {
                let expr = simplify_expr(expr, schema)?;
                if expr.transformed {
                    Transformed::yes(expr.data.not())
                } else {
                    Transformed::no(expr.data.not())
                }
            }
        },
        // expr IN () --> false
        Expr::IsIn(_, list) if list.is_empty() => Transformed::yes(lit(false)),

        other => Transformed::no(Arc::new(other)),
    })
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

fn is_true(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(LiteralValue::Boolean(v)) => *v,
        _ => false,
    }
}
fn is_false(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(LiteralValue::Boolean(v)) => !*v,
        _ => false,
    }
}

/// returns true if expr is a
/// `Expr::Literal(LiteralValue::Boolean(v))` , false otherwise
fn is_bool_lit(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(LiteralValue::Boolean(_)))
}

fn is_bool_type(expr: &Expr, schema: &SchemaRef) -> bool {
    matches!(expr.get_type(schema), Ok(DataType::Boolean))
}

fn as_bool_lit(expr: &Expr) -> Option<bool> {
    expr.as_literal().and_then(|l| l.as_bool())
}

fn is_null(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(LiteralValue::Null))
}

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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_dsl::{col, lit, null_lit, ExprRef};
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
    };
    use rstest::{fixture, rstest};

    use crate::simplify_expr;

    #[fixture]
    fn schema() -> SchemaRef {
        Arc::new(
            Schema::new(vec![
                Field::new("bool", DataType::Boolean),
                Field::new("int", DataType::Int32),
            ])
            .unwrap(),
        )
    }

    #[rstest]
    // true = A  --> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // false = A --> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // A == true ---> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // null = A --> null
    #[case(null_lit().eq(col("bool")), null_lit())]
    // A == false ---> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // true != A  --> !A
    #[case(lit(true).not_eq(col("bool")), col("bool").not())]
    // false != A --> A
    #[case(lit(false).not_eq(col("bool")), col("bool"))]
    // true OR A  --> true
    #[case(lit(true).or(col("bool")), lit(true))]
    // false OR A  --> A
    #[case(lit(false).or(col("bool")), col("bool"))]
    // A OR true  --> true
    #[case(col("bool").or(lit(true)), lit(true))]
    // A OR false --> A
    #[case(col("bool").or(lit(false)), col("bool"))]
    fn test_simplify_bool_exprs(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(Arc::unwrap_or_clone(input), &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    // A * 1 --> A
    #[case(col("int").mul(lit(1)), col("int"))]
    // 1 * A --> A
    #[case(lit(1).mul(col("int")), col("int"))]
    // A / 1 --> A
    #[case(col("int").div(lit(1)), col("int"))]
    // A + 0 --> A
    #[case(col("int").add(lit(0)), col("int"))]
    // A - 0 --> A
    #[case(col("int").sub(lit(0)), col("int"))]
    fn test_math_exprs(
        #[case] input: ExprRef,
        #[case] expected: ExprRef,
        schema: SchemaRef,
    ) -> DaftResult<()> {
        let optimized = simplify_expr(Arc::unwrap_or_clone(input), &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    fn test_not_between(schema: SchemaRef) -> DaftResult<()> {
        let input = col("int").between(lit(1), lit(10)).not();
        let expected = col("int").lt(lit(1)).or(col("int").gt(lit(10)));

        let optimized = simplify_expr(Arc::unwrap_or_clone(input), &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }

    #[rstest]
    fn test_between(schema: SchemaRef) -> DaftResult<()> {
        let input = col("int").between(lit(1), lit(10));
        let expected = col("int").lt_eq(lit(10)).and(col("int").gt_eq(lit(1)));

        let optimized = simplify_expr(Arc::unwrap_or_clone(input), &schema)?;

        assert!(optimized.transformed);
        assert_eq!(optimized.data, expected);
        Ok(())
    }
}
