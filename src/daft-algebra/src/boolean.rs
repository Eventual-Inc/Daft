use std::sync::Arc;

use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_dsl::{Expr, ExprRef, Operator};

pub fn split_conjunction(expr: &ExprRef) -> Vec<ExprRef> {
    let mut splits = vec![];

    expr.apply(|e| match e.as_ref() {
        Expr::BinaryOp {
            op: Operator::And, ..
        }
        | Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
        _ => {
            splits.push(e.clone());
            Ok(TreeNodeRecursion::Jump)
        }
    })
    .unwrap();

    splits
}

pub fn split_disjunction(expr: &ExprRef) -> Vec<ExprRef> {
    let mut splits = vec![];

    expr.apply(|e| match e.as_ref() {
        Expr::BinaryOp {
            op: Operator::Or, ..
        }
        | Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
        _ => {
            splits.push(e.clone());
            Ok(TreeNodeRecursion::Jump)
        }
    })
    .unwrap();

    splits
}

pub fn combine_conjunction<T: IntoIterator<Item = ExprRef>>(exprs: T) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, e| acc.and(e))
}

pub fn combine_disjunction<T: IntoIterator<Item = ExprRef>>(exprs: T) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, e| acc.or(e))
}

/// Converts a boolean expression to conjunctive normal form (AND of ORs)
pub fn to_cnf(expr: ExprRef) -> ExprRef {
    let dnf_form = to_dnf(expr.not()).not();

    apply_de_morgans(dnf_form).data
}

/// Converts a boolean expression to disjunctive normal form (OR of ANDs)
pub fn to_dnf(expr: ExprRef) -> ExprRef {
    let dm_expr = apply_de_morgans(expr).data;

    // apply distributive property recursively
    dm_expr
        .transform_up(|e| {
            Ok(
                if let Expr::BinaryOp {
                    op: Operator::And,
                    left,
                    right,
                } = e.as_ref()
                {
                    let left_exprs = split_disjunction(left);
                    let right_exprs = split_disjunction(right);

                    if left_exprs.len() == 1 && right_exprs.len() == 1 {
                        Transformed::no(e)
                    } else {
                        let conditions = left_exprs
                            .iter()
                            .flat_map(|l| right_exprs.iter().map(|r| l.clone().and(r.clone())));

                        Transformed::yes(combine_disjunction(conditions).unwrap())
                    }
                } else {
                    Transformed::no(e)
                },
            )
        })
        .unwrap()
        .data
}

/// Push all negations into boolean atoms by applying De Morgan's law + eliminate double negations
fn apply_de_morgans(expr: ExprRef) -> Transformed<ExprRef> {
    fn transform_fn(e: ExprRef) -> Transformed<ExprRef> {
        match e.as_ref() {
            Expr::Not(ne) => match ne.as_ref() {
                // !!x -> x
                Expr::Not(nne) => {
                    // we do our own recursion here to not skip triple negatives
                    Transformed::yes(transform_fn(nne.clone()).data)
                }
                // !(x & y) -> ((!x) | (!y))
                Expr::BinaryOp {
                    op: Operator::And,
                    left,
                    right,
                } => Transformed::yes(Arc::new(Expr::BinaryOp {
                    op: Operator::Or,
                    left: left.clone().not(),
                    right: right.clone().not(),
                })),
                // !(x | y) -> ((!x) & (!y))
                Expr::BinaryOp {
                    op: Operator::Or,
                    left,
                    right,
                } => Transformed::yes(Arc::new(Expr::BinaryOp {
                    op: Operator::And,
                    left: left.clone().not(),
                    right: right.clone().not(),
                })),
                _ => Transformed::no(e),
            },
            _ => Transformed::no(e),
        }
    }

    expr.transform_down(|e| Ok(transform_fn(e))).unwrap()
}

#[cfg(test)]
mod tests {
    use daft_dsl::resolved_col;

    use crate::boolean::{to_cnf, to_dnf};

    #[test]
    fn dnf_simple() {
        // a & (b | c) -> (a & b) | (a & c)
        let expr = resolved_col("a").and(resolved_col("b").or(resolved_col("c")));
        let expected = resolved_col("a")
            .and(resolved_col("b"))
            .or(resolved_col("a").and(resolved_col("c")));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_simple() {
        // a | (b & c) -> (a | b) & (a | c)
        let expr = resolved_col("a").or(resolved_col("b").and(resolved_col("c")));
        let expected = resolved_col("a")
            .or(resolved_col("b"))
            .and(resolved_col("a").or(resolved_col("c")));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_neg() {
        // !(a & ((!b) | c)) -> (!a) | (b & (!c))
        let expr = resolved_col("a")
            .and(resolved_col("b").not().or(resolved_col("c")))
            .not();
        let expected = resolved_col("a")
            .not()
            .or(resolved_col("b").and(resolved_col("c").not()));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_neg() {
        // !(a | ((!b) & c)) -> (!a) & (b | (!c))
        let expr = resolved_col("a")
            .or(resolved_col("b").not().and(resolved_col("c")))
            .not();
        let expected = resolved_col("a")
            .not()
            .and(resolved_col("b").or(resolved_col("c").not()));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_nested() {
        // a & b & ((c & d) | (e & f)) -> (a & b & c & d) | (a & b & e & f)
        let expr = resolved_col("a").and(resolved_col("b")).and(
            (resolved_col("c").and(resolved_col("d"))).or(resolved_col("e").and(resolved_col("f"))),
        );
        let expected = (resolved_col("a")
            .and(resolved_col("b"))
            .and(resolved_col("c").and(resolved_col("d"))))
        .or(resolved_col("a")
            .and(resolved_col("b"))
            .and(resolved_col("e").and(resolved_col("f"))));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_nested() {
        // a & b & ((c & d) | (e & f)) -> a & b & (c | e) & (c | f) & (d | e) & (d | f)
        let expr = resolved_col("a").and(resolved_col("b")).and(
            (resolved_col("c").and(resolved_col("d"))).or(resolved_col("e").and(resolved_col("f"))),
        );
        let expected = resolved_col("a").and(resolved_col("b")).and(
            (resolved_col("c").or(resolved_col("e")))
                .and(resolved_col("c").or(resolved_col("f")))
                .and(resolved_col("d").or(resolved_col("e")))
                .and(resolved_col("d").or(resolved_col("f"))),
        );

        assert_eq!(expected, to_cnf(expr));
    }
}
