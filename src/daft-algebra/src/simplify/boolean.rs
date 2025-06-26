use common_error::DaftResult;
use common_treenode::Transformed;
use daft_dsl::{lit, Expr, ExprRef, LiteralValue, Operator};
use daft_schema::{dtype::DataType, schema::SchemaRef};
use indexmap::IndexSet;

use crate::boolean::{
    combine_conjunction, combine_disjunction, split_conjunction, split_disjunction,
};

fn is_true(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(LiteralValue::Boolean(true)))
}

fn is_false(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(LiteralValue::Boolean(false)))
}

fn is_bool(expr: &Expr, schema: &SchemaRef) -> DaftResult<bool> {
    Ok(matches!(expr.get_type(schema)?, DataType::Boolean))
}

/// Simplify boolean expressions (AND, OR, NOT, etc.)
pub(crate) fn simplify_boolean_expr(
    expr: ExprRef,
    _schema: &SchemaRef,
) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::BinaryOp { op, left, right } => match op {
            // true AND e -> e
            Operator::And if is_true(left) => Transformed::yes(right.clone()),
            Operator::And if is_true(right) => Transformed::yes(left.clone()),
            // false AND e -> false
            Operator::And if is_false(left) => Transformed::yes(lit(false)),
            Operator::And if is_false(right) => Transformed::yes(lit(false)),
            // false OR e -> e
            Operator::Or if is_false(left) => Transformed::yes(right.clone()),
            Operator::Or if is_false(right) => Transformed::yes(left.clone()),
            // true OR e -> true
            Operator::Or if is_true(left) => Transformed::yes(lit(true)),
            Operator::Or if is_true(right) => Transformed::yes(lit(true)),

            Operator::And => simplify_and(expr.clone(), left, right),
            Operator::Or => simplify_or(expr.clone(), left, right),

            _ => Transformed::no(expr),
        },
        Expr::Not(not) => {
            match not.as_ref() {
                // !!e -> e
                Expr::Not(not_not) => Transformed::yes(not_not.clone()),
                // !true -> false, !false -> true
                Expr::Literal(LiteralValue::Boolean(val)) => Transformed::yes(lit(!val)),
                _ => Transformed::no(expr),
            }
        }
        _ => Transformed::no(expr),
    })
}

/// Combines common sub-expressions in AND expressions.
///
/// 1. Matches up each expression in the left-side conjunction with each expression in the right-side conjunction.
///
/// (We only match expressions between left and right sides and not within each side because this rule is run bottom-up,
/// so the sides should have already been individually simplified.)
///
/// 2. For each pair, check if there are any common expressions in their disjunctions.
///
///     Ex:
///     - left: (a OR b OR c OR ...)
///     - right: (a OR b OR d OR ...)
///     - common: a, b
///
/// 3. If there are common expressions, extract them out into ORs.
///
///     Ex (using above example):
///     - a OR b OR ((c OR ...) AND (d OR ...))
///
/// 4. Finally, combine all new and remaining expressions back into conjunction
fn simplify_and(expr: ExprRef, left: &ExprRef, right: &ExprRef) -> Transformed<ExprRef> {
    let left_and_exprs = split_conjunction(left);
    let right_and_exprs = split_conjunction(right);

    let left_and_of_or_exprs = left_and_exprs
        .iter()
        .map(|e| split_disjunction(e).into_iter().collect())
        .collect::<Vec<IndexSet<_>>>();
    let right_and_of_or_exprs = right_and_exprs
        .iter()
        .map(|e| split_disjunction(e).into_iter().collect())
        .collect::<Vec<IndexSet<_>>>();

    let mut left_eliminated = vec![false; left_and_exprs.len()];
    let mut right_eliminated = vec![false; right_and_exprs.len()];
    let mut simplified_exprs = Vec::new();

    for (i, left_exprs) in left_and_of_or_exprs.iter().enumerate() {
        for (j, right_exprs) in right_and_of_or_exprs.iter().enumerate() {
            if right_eliminated[j] || left_exprs.is_disjoint(right_exprs) {
                continue;
            }

            let common_exprs: IndexSet<_> = left_exprs.intersection(right_exprs).cloned().collect();

            let simplified = if common_exprs == *left_exprs || common_exprs == *right_exprs {
                // (a OR b OR c OR ...) AND (a OR b) -> (a OR b)
                combine_disjunction(common_exprs).unwrap()
            } else {
                // (a OR b OR c OR ...) AND (a OR b OR d OR ...) -> a OR b OR ((c OR ...) AND (d OR ...))
                let left_remaining =
                    combine_disjunction(left_exprs.difference(&common_exprs).cloned()).unwrap();
                let right_remaining =
                    combine_disjunction(right_exprs.difference(&common_exprs).cloned()).unwrap();

                let common_disjunction = combine_disjunction(common_exprs).unwrap();

                common_disjunction.or(left_remaining.and(right_remaining))
            };

            simplified_exprs.push(simplified);
            left_eliminated[i] = true;
            right_eliminated[j] = true;
            break;
        }
    }

    if simplified_exprs.is_empty() {
        Transformed::no(expr)
    } else {
        let left_and_remaining = left_and_exprs
            .into_iter()
            .zip(left_eliminated)
            .filter(|(_, eliminated)| !*eliminated)
            .map(|(e, _)| e);

        let right_and_remaining = right_and_exprs
            .into_iter()
            .zip(right_eliminated)
            .filter(|(_, eliminated)| !*eliminated)
            .map(|(e, _)| e);

        Transformed::yes(
            combine_conjunction(
                simplified_exprs
                    .into_iter()
                    .chain(left_and_remaining)
                    .chain(right_and_remaining),
            )
            .unwrap(),
        )
    }
}

/// Combines common sub-expressions in OR expressions.
///
/// 1. Matches up each expression in the left-side disjunction with each expression in the right-side disjunction.
///
/// (We only match expressions between left and right sides and not within each side because this rule is run bottom-up,
/// so the sides should have already been individually simplified.)
///
/// 2. For each pair, check if there are any common expressions in their conjunctions.
///
///     Ex:
///     - left: (a AND b AND c AND ...)
///     - right: (a AND b AND d AND ...)
///     - common: a, b
///
/// 3. If there are common expressions, extract them out into ANDs.
///
///     Ex (using above example):
///     - a AND b AND ((c AND ...) OR (d AND ...))
///
/// 4. Finally, combine all new and remaining expressions back into disjunction
fn simplify_or(expr: ExprRef, left: &ExprRef, right: &ExprRef) -> Transformed<ExprRef> {
    let left_or_exprs = split_disjunction(left);
    let right_or_exprs = split_disjunction(right);

    let left_or_of_and_exprs = left_or_exprs
        .iter()
        .map(|e| split_conjunction(e).into_iter().collect())
        .collect::<Vec<IndexSet<_>>>();
    let right_or_of_and_exprs = right_or_exprs
        .iter()
        .map(|e| split_conjunction(e).into_iter().collect())
        .collect::<Vec<IndexSet<_>>>();

    let mut left_eliminated = vec![false; left_or_exprs.len()];
    let mut right_eliminated = vec![false; right_or_exprs.len()];
    let mut simplified_exprs = Vec::new();

    for (i, left_exprs) in left_or_of_and_exprs.iter().enumerate() {
        for (j, right_exprs) in right_or_of_and_exprs.iter().enumerate() {
            if right_eliminated[j] || left_exprs.is_disjoint(right_exprs) {
                continue;
            }

            let common_exprs: IndexSet<_> = left_exprs.intersection(right_exprs).cloned().collect();

            let simplified = if common_exprs == *left_exprs || common_exprs == *right_exprs {
                // (a AND b AND c AND ...) OR (a AND b) -> (a AND b)
                combine_conjunction(common_exprs).unwrap()
            } else {
                // (a AND b AND c AND ...) OR (a AND b AND d AND ...) -> a AND b AND ((c AND ...) OR (d AND ...))
                let left_remaining =
                    combine_conjunction(left_exprs.difference(&common_exprs).cloned()).unwrap();
                let right_remaining =
                    combine_conjunction(right_exprs.difference(&common_exprs).cloned()).unwrap();

                let common_conjunction = combine_conjunction(common_exprs).unwrap();

                common_conjunction.and(left_remaining.or(right_remaining))
            };

            simplified_exprs.push(simplified);
            left_eliminated[i] = true;
            right_eliminated[j] = true;
            break;
        }
    }

    if simplified_exprs.is_empty() {
        Transformed::no(expr)
    } else {
        let left_or_remaining = left_or_exprs
            .into_iter()
            .zip(left_eliminated)
            .filter(|(_, eliminated)| !*eliminated)
            .map(|(e, _)| e);

        let right_or_remaining = right_or_exprs
            .into_iter()
            .zip(right_eliminated)
            .filter(|(_, eliminated)| !*eliminated)
            .map(|(e, _)| e);

        Transformed::yes(
            combine_disjunction(
                simplified_exprs
                    .into_iter()
                    .chain(left_or_remaining)
                    .chain(right_or_remaining),
            )
            .unwrap(),
        )
    }
}

/// Simplify binary comparison ops (==, !=)
pub(crate) fn simplify_binary_compare(
    expr: ExprRef,
    schema: &SchemaRef,
) -> DaftResult<Transformed<ExprRef>> {
    Ok(match expr.as_ref() {
        Expr::BinaryOp { op, left, right } => match op {
            // true = e -> e
            Operator::Eq if is_true(left) && is_bool(right, schema)? => {
                Transformed::yes(right.clone())
            }
            Operator::Eq if is_true(right) && is_bool(left, schema)? => {
                Transformed::yes(left.clone())
            }
            // false = e -> !e
            Operator::Eq if is_false(left) && is_bool(right, schema)? => {
                Transformed::yes(right.clone().not())
            }
            Operator::Eq if is_false(right) && is_bool(left, schema)? => {
                Transformed::yes(left.clone().not())
            }

            // true != e -> !e
            Operator::NotEq if is_true(left) && is_bool(right, schema)? => {
                Transformed::yes(right.clone().not())
            }
            Operator::NotEq if is_true(right) && is_bool(left, schema)? => {
                Transformed::yes(left.clone().not())
            }
            // false != e -> e
            Operator::NotEq if is_false(left) && is_bool(right, schema)? => {
                Transformed::yes(right.clone())
            }
            Operator::NotEq if is_false(right) && is_bool(left, schema)? => {
                Transformed::yes(left.clone())
            }

            _ => Transformed::no(expr),
        },
        _ => Transformed::no(expr),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use common_treenode::Transformed;
    use daft_dsl::{lit, Column, Expr, ExprRef, Operator, ResolvedColumn};
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use crate::simplify::boolean::simplify_binary_compare;

    // Helper function to create a column reference.
    fn col(name: &str) -> ExprRef {
        Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::Basic(
            Arc::<str>::from(name),
        ))))
    }

    // Helper functions for various comparison operators.
    fn eq(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::Eq,
            left,
            right,
        })
    }

    fn neq(left: ExprRef, right: ExprRef) -> ExprRef {
        Arc::new(Expr::BinaryOp {
            op: Operator::NotEq,
            left,
            right,
        })
    }
    #[test]
    fn test_simplify_binary_compare() -> DaftResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean)]));
        let col_a = col("a");

        // a == true
        let expr = eq(col_a.clone(), lit(true));
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone()));

        let expr = eq(lit(true), col_a.clone());
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone()));

        // a == false
        let expr = eq(col_a.clone(), lit(false));
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone().not()));

        let expr = eq(lit(false), col_a.clone());
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone().not()));

        // a != true
        let expr = neq(col_a.clone(), lit(true));
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone().not()));

        let expr = neq(lit(true), col_a.clone());
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone().not()));

        // a != false
        let expr = neq(col_a.clone(), lit(false));
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone()));

        let expr = neq(lit(false), col_a.clone());
        let result = simplify_binary_compare(expr.clone(), &schema)?;
        assert_eq!(result, Transformed::yes(col_a.clone()));

        Ok(())
    }
}
