use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::DynTreeNode;

use crate::{Expr, ExprRef};

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `Expr::children`.
fn with_new_children_if_necessary(expr: ExprRef, children: Vec<ExprRef>) -> DaftResult<ExprRef> {
    let old_children = expr.children();
    if children.len() != old_children.len() {
        panic!("Expr::with_new_children_if_necessary: Wrong number of children")
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        Ok(expr.with_new_children(children).arced())
    } else {
        Ok(expr)
    }
}

impl DynTreeNode for Expr {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> DaftResult<Arc<Self>> {
        with_new_children_if_necessary(arc_self, new_children)
    }
}
