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

pub fn combine_conjunction<T: IntoIterator<Item = ExprRef>>(exprs: T) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, e| acc.and(e))
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
        .transform_down(|e| {
            Ok(match e.as_ref() {
                Expr::BinaryOp {
                    op: Operator::And,
                    left,
                    right,
                } => {
                    if let Expr::BinaryOp {
                        op: Operator::Or,
                        left: right_left,
                        right: right_right,
                    } = right.as_ref()
                    {
                        // (x & (y | z)) -> ((x & y) | (x & z))
                        Transformed::yes(Arc::new(Expr::BinaryOp {
                            op: Operator::Or,
                            left: Arc::new(Expr::BinaryOp {
                                op: Operator::And,
                                left: left.clone(),
                                right: right_left.clone(),
                            }),
                            right: Arc::new(Expr::BinaryOp {
                                op: Operator::And,
                                left: left.clone(),
                                right: right_right.clone(),
                            }),
                        }))
                    } else if let Expr::BinaryOp {
                        op: Operator::Or,
                        left: left_left,
                        right: left_right,
                    } = left.as_ref()
                    {
                        // ((x | y) & z) -> ((x & z) | (y & z))
                        Transformed::yes(Arc::new(Expr::BinaryOp {
                            op: Operator::Or,
                            left: Arc::new(Expr::BinaryOp {
                                op: Operator::And,
                                left: left_left.clone(),
                                right: right.clone(),
                            }),
                            right: Arc::new(Expr::BinaryOp {
                                op: Operator::And,
                                left: left_right.clone(),
                                right: right.clone(),
                            }),
                        }))
                    } else {
                        Transformed::no(e)
                    }
                }
                _ => Transformed::no(e),
            })
        })
        .unwrap()
        .data
}

/// Transform boolean expression by applying De Morgan's law + eliminate double negations recursively
fn apply_de_morgans(expr: ExprRef) -> Transformed<ExprRef> {
    expr.transform_down(|e| {
        Ok(match e.as_ref() {
            Expr::Not(ne) => match ne.as_ref() {
                // !x -> x
                Expr::Not(nne) => Transformed::yes(nne.clone()),
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
        })
    })
    .unwrap()
}

#[cfg(test)]
mod tests {
    use daft_dsl::col;

    use crate::boolean::{to_cnf, to_dnf};

    #[test]
    fn dnf_simple() {
        // a & (b | c) -> (a & b) | (a & c)
        let expr = col("a").and(col("b").or(col("c")));
        let expected = col("a").and(col("b")).or(col("a").and(col("c")));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_simple() {
        // a | (b & c) -> (a | b) & (a | c)
        let expr = col("a").or(col("b").and(col("c")));
        let expected = col("a").or(col("b")).and(col("a").or(col("c")));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_neg() {
        // !(a & ((!b) | c)) -> (!a) | (b & (!c))
        let expr = col("a").and(col("b").not().or(col("c"))).not();
        let expected = col("a").not().or(col("b").and(col("c").not()));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_neg() {
        // !(a | ((!b) & c)) -> (!a) & (b | (!c))
        let expr = col("a").or(col("b").not().and(col("c"))).not();
        let expected = col("a").not().and(col("b").or(col("c").not()));

        assert_eq!(expected, to_cnf(expr));
    }

    #[test]
    fn dnf_nested() {
        // a & b & ((c & d) | (e & f)) -> (a & b & c & d) | (a & b & e & f)
        let expr = col("a")
            .and(col("b"))
            .and((col("c").and(col("d"))).or(col("e").and(col("f"))));
        let expected = (col("a").and(col("b")).and(col("c").and(col("d"))))
            .or(col("a").and(col("b")).and(col("e").and(col("f"))));

        assert_eq!(expected, to_dnf(expr));
    }

    #[test]
    fn cnf_nested() {
        // a & b & ((c & d) | (e & f)) -> a & b & (c | e) & (c | f) & (d | e) & (d | f)
        let expr = col("a")
            .and(col("b"))
            .and((col("c").and(col("d"))).or(col("e").and(col("f"))));
        let expected = col("a").and(col("b")).and(
            (col("c").or(col("e")))
                .and(col("d").or(col("e")))
                .and(col("c").or(col("f")).and(col("d").or(col("f")))),
        );

        assert_eq!(expected, to_cnf(expr));
    }
}
