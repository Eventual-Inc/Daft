use std::collections::HashMap;

use common_error::DaftResult;
use common_treenode::{
    RewriteRecursion, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};

use super::expr::Expr;

struct RequiredColumnVisitor {
    required: Vec<String>,
}

impl TreeNodeVisitor for RequiredColumnVisitor {
    type N = Expr;
    fn pre_visit(&mut self, node: &Self::N) -> DaftResult<VisitRecursion> {
        if let Expr::Column(name) = node {
            self.required.push(name.as_ref().into());
        };
        Ok(VisitRecursion::Continue)
    }
}

pub fn get_required_columns(e: &Expr) -> Vec<String> {
    let mut visitor = RequiredColumnVisitor { required: vec![] };
    e.visit(&mut visitor)
        .expect("Error occurred when visiting for required columns");
    visitor.required
}

pub fn requires_computation(e: &Expr) -> bool {
    // Returns whether or not this expression runs any computation on the underlying data
    match e {
        Expr::Alias(child, _) => requires_computation(child),
        Expr::Column(..) | Expr::Literal(_) => false,
        Expr::Agg(..)
        | Expr::BinaryOp { .. }
        | Expr::Cast(..)
        | Expr::Function { .. }
        | Expr::Not(..)
        | Expr::IsNull(..)
        | Expr::IfElse { .. } => true,
    }
}

struct ColumnExpressionRewriter<'a> {
    mapping: &'a HashMap<String, Expr>,
}

impl<'a> TreeNodeRewriter for ColumnExpressionRewriter<'a> {
    type N = Expr;
    fn pre_visit(&mut self, node: &Self::N) -> DaftResult<RewriteRecursion> {
        if let Expr::Column(name) = node && self.mapping.contains_key(name.as_ref()) {
            Ok(RewriteRecursion::Continue)
        } else {
            Ok(RewriteRecursion::Skip)
        }
    }
    fn mutate(&mut self, node: Self::N) -> DaftResult<Self::N> {
        if let Expr::Column(ref name) = node && let Some(tgt) = self.mapping.get(name.as_ref()){
            Ok(tgt.clone())
        } else {
            Ok(node)
        }
    }
}

pub fn replace_columns_with_expressions(expr: &Expr, replace_map: &HashMap<String, Expr>) -> Expr {
    let mut column_rewriter = ColumnExpressionRewriter {
        mapping: replace_map,
    };
    expr.clone()
        .rewrite(&mut column_rewriter)
        .expect("Error occurred when rewriting column expressions")
}
