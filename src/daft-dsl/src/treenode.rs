use common_error::DaftResult;
use common_treenode::{TreeNode, VisitRecursion};

use crate::Expr;

impl TreeNode for Expr {
    fn apply_children<F>(&self, op: &mut F) -> DaftResult<common_treenode::VisitRecursion>
    where
        F: FnMut(&Self) -> DaftResult<common_treenode::VisitRecursion>,
    {
        use Expr::*;
        let children = match self {
            Alias(expr, _) | Cast(expr, _) | Not(expr) | IsNull(expr) => {
                vec![expr.as_ref()]
            }
            Agg(agg_expr) => {
                use crate::AggExpr::*;
                match agg_expr {
                    Count(expr, ..)
                    | Sum(expr)
                    | Mean(expr)
                    | Min(expr)
                    | Max(expr)
                    | List(expr)
                    | Concat(expr) => vec![expr.as_ref()],
                }
            }
            BinaryOp { op: _, left, right } => vec![left.as_ref(), right.as_ref()],
            Column(_) | Literal(_) => vec![],
            Function { func: _, inputs } => inputs.iter().collect::<Vec<_>>(),
            IfElse {
                if_true,
                if_false,
                predicate,
            } => vec![if_true.as_ref(), if_false.as_ref(), predicate.as_ref()],
        };
        for child in children.into_iter() {
            match op(child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> DaftResult<Self>
    where
        F: FnMut(Self) -> DaftResult<Self>,
    {
        let mut transform = transform;

        use Expr::*;
        Ok(match self {
            Alias(expr, name) => Alias(transform(expr.as_ref().clone())?.into(), name),
            Column(_) | Literal(_) => self,
            Cast(expr, dtype) => Cast(transform(expr.as_ref().clone())?.into(), dtype),
            Agg(agg_expr) => {
                use crate::AggExpr::*;
                match agg_expr {
                    Count(expr, mode) => transform(expr.as_ref().clone())?.count(mode),
                    Sum(expr) => transform(expr.as_ref().clone())?.sum(),
                    Mean(expr) => transform(expr.as_ref().clone())?.mean(),
                    Min(expr) => transform(expr.as_ref().clone())?.min(),
                    Max(expr) => transform(expr.as_ref().clone())?.max(),
                    List(expr) => transform(expr.as_ref().clone())?.agg_list(),
                    Concat(expr) => transform(expr.as_ref().clone())?.agg_concat(),
                }
            }
            Not(expr) => Not(transform(expr.as_ref().clone())?.into()),
            IsNull(expr) => IsNull(transform(expr.as_ref().clone())?.into()),
            IfElse {
                if_true,
                if_false,
                predicate,
            } => Expr::IfElse {
                if_true: transform(if_true.as_ref().clone())?.into(),
                if_false: transform(if_false.as_ref().clone())?.into(),
                predicate: transform(predicate.as_ref().clone())?.into(),
            },
            BinaryOp { op, left, right } => Expr::BinaryOp {
                op,
                left: transform(left.as_ref().clone())?.into(),
                right: transform(right.as_ref().clone())?.into(),
            },
            Function { func, inputs } => Function {
                func,
                inputs: inputs
                    .into_iter()
                    .map(transform)
                    .collect::<DaftResult<Vec<_>>>()?,
            },
        })
    }
}
