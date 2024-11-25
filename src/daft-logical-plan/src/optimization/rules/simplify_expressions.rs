use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::PhysicalScanInfo;
use common_treenode::{Transformed, TreeNode};
use daft_core::prelude::SchemaRef;
use daft_dsl::{lit, null_lit, Expr, ExprRef, LiteralValue, Operator};
use daft_schema::dtype::DataType;

use super::OptimizerRule;
use crate::LogicalPlan;

/// Optimization rule for simplifying expressions
#[derive(Default, Debug)]
pub struct SimplifyExpressionsRule {}

impl SimplifyExpressionsRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SimplifyExpressionsRule {
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>> {
        if plan.exists(|p| match p.as_ref() {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                crate::SourceInfo::Physical(PhysicalScanInfo { scan_op, .. })
                    // TODO: support simplify expressions for SQLScanOperator
                    if scan_op.0.name() == "SQLScanOperator" =>
                {
                    true
                }
                _ => false,
            },
            _ => false,
        }) {
            return Ok(Transformed::no(plan));
        }
        let schema = plan.schema();
        Ok(Arc::unwrap_or_clone(plan)
            .map_expressions(|expr| simplify_expr(Arc::unwrap_or_clone(expr), &schema))?
            .update_data(Arc::new))
    }
}

fn simplify_expr(expr: Expr, schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr {
        // ----------------
        // Eq
        // ----------------

        // true = A  --> A
        // false = A --> !A
        // null = A --> null
        Expr::BinaryOp {
            op: Operator::Eq,
            left,
            right,
        } if is_bool_lit(&left) && is_bool_type(&right, schema) => {
            Transformed::yes(match as_bool_lit(&left) {
                Some(true) => right,
                Some(false) => right.not(),
                None => null_lit(),
            })
        }
        // A = true --> A
        // A = false --> !A
        // A = null --> null
        Expr::BinaryOp {
            op: Operator::Eq,
            left,
            right,
        } if is_bool_lit(&right) && is_bool_type(&left, schema) => {
            Transformed::yes(match as_bool_lit(&right) {
                Some(true) => left,
                Some(false) => left.not(),
                None => null_lit(),
            })
        }
        // ----------------
        // Neq
        // ----------------

        // true != A  --> !A
        // false != A --> A
        // null != A --> null
        Expr::BinaryOp {
            op: Operator::NotEq,
            left,
            right,
        } if is_bool_lit(&left) && is_bool_type(&right, schema) => {
            Transformed::yes(match as_bool_lit(&left) {
                Some(true) => right.not(),
                Some(false) => right,
                None => null_lit(),
            })
        }

        // A != true --> !A
        // A != false --> A
        // A != null --> null
        Expr::BinaryOp {
            op: Operator::NotEq,
            left,
            right,
        } if is_bool_lit(&right) && is_bool_type(&left, schema) => {
            Transformed::yes(match as_bool_lit(&right) {
                Some(true) => left.not(),
                Some(false) => left,
                None => null_lit(),
            })
        }

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
        Expr::BinaryOp {
            op: Operator::Multiply,
            left,
            right,
        } if is_one(&right) => Transformed::yes(left),

        // 1 * A --> A
        Expr::BinaryOp {
            op: Operator::Multiply,
            left,
            right,
        } if is_one(&left) => Transformed::yes(right),

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
            let expr = simplify_expr(Arc::unwrap_or_clone(expr), schema)?.data;
            let low = simplify_expr(Arc::unwrap_or_clone(low), schema)?.data;
            let high = simplify_expr(Arc::unwrap_or_clone(high), schema)?.data;
            Transformed::yes(expr.clone().lt_eq(high).and(expr.gt_eq(low)))
        }
        Expr::Not(expr) => match Arc::unwrap_or_clone(expr) {
            // NOT (BETWEEN A AND B) --> A < low OR A > high
            Expr::Between(expr, low, high) => {
                let expr = simplify_expr(Arc::unwrap_or_clone(expr), schema)?.data;
                let low = simplify_expr(Arc::unwrap_or_clone(low), schema)?.data;
                let high = simplify_expr(Arc::unwrap_or_clone(high), schema)?.data;

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

    use daft_core::prelude::{Schema, TimeUnit};
    use daft_dsl::{col, lit, ExprRef};
    use daft_schema::{dtype::DataType, field::Field};
    use rstest::rstest;

    use super::SimplifyExpressionsRule;
    use crate::{
        ops::{Filter, Source},
        optimization::rules::OptimizerRule,
        source_info::PlaceHolderInfo,
        ClusteringSpec, LogicalPlan, LogicalPlanBuilder, SourceInfo,
    };

    fn make_source() -> LogicalPlanBuilder {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("test", DataType::Utf8),
                Field::new("utf8", DataType::Utf8),
                Field::new("i32", DataType::Int32),
                Field::new("i64", DataType::Int64),
                Field::new("f32", DataType::Float32),
                Field::new("f64", DataType::Float64),
                Field::new("bool", DataType::Boolean),
                Field::new("date", DataType::Date),
                Field::new("time", DataType::Time(TimeUnit::Microseconds)),
                Field::new("list_utf8", DataType::new_list(DataType::Utf8)),
            ])
            .unwrap(),
        );
        LogicalPlanBuilder::from(
            LogicalPlan::Source(Source {
                output_schema: schema.clone(),
                source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                    source_schema: schema,
                    clustering_spec: Arc::new(ClusteringSpec::unknown()),
                    source_id: 0,
                })),
            })
            .arced(),
        )
    }

    #[rstest]
    // true = A  --> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // false = A --> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // A == true ---> A
    #[case(col("bool").eq(lit(true)), col("bool"))]
    // A == false ---> !A
    #[case(col("bool").eq(lit(false)), col("bool").not())]
    // true != A  --> !A
    #[case(lit(true).not_eq(col("bool")), col("bool").not())]
    // false != A --> A
    #[case(lit(false).not_eq(col("bool")), col("bool"))]
    fn test_simplify_bool_exprs(#[case] input: ExprRef, #[case] expected: ExprRef) {
        let source = make_source().filter(input).unwrap().build();
        let optimizer = SimplifyExpressionsRule::new();
        let optimized = optimizer.try_optimize(source).unwrap();

        // make sure the expression is simplified
        assert!(optimized.transformed);
        let LogicalPlan::Filter(Filter {
            input: _,
            predicate,
        }) = optimized.data.as_ref()
        else {
            panic!("Expected Filter, got {:?}", optimized.data)
        };

        assert_eq!(predicate, &expected);
    }
}
