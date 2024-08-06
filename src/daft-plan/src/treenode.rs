use std::sync::Arc;

use crate::{physical_plan::PhysicalPlan, LogicalPlan};
use common_error::DaftResult;
use common_treenode::{DynTreeNode, Transformed, TreeNode, TreeNodeIterator};

impl TreeNode for LogicalPlan {
    fn apply_children<F: FnMut(&Self) -> DaftResult<common_treenode::TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> DaftResult<common_treenode::TreeNodeRecursion> {
        self.children()
            .into_iter()
            .apply_until_stop(|node| f(node.as_ref()))
    }

    fn map_children<F: FnMut(Self) -> DaftResult<common_treenode::Transformed<Self>>>(
        self,
        mut f: F,
    ) -> DaftResult<common_treenode::Transformed<Self>> {
        let children = self.children();
        if !children.is_empty() {
            let new_children = children.into_iter().map_until_stop_and_collect(|plan| {
                f(unwrap_arc(plan))?.map_data(|new_plan| Ok(Arc::new(new_plan)))
            })?;
            if new_children.transformed {
                new_children
                    .map_data(|new_children| Ok(self.with_new_children(new_children.as_ref())))
            } else {
                Ok(Transformed::new(self, false, new_children.tnr))
            }
        } else {
            Ok(Transformed::no(self))
        }
    }
}

impl TreeNode for PhysicalPlan {
    fn apply_children<F: FnMut(&Self) -> DaftResult<common_treenode::TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> DaftResult<common_treenode::TreeNodeRecursion> {
        self.children()
            .into_iter()
            .apply_until_stop(|node| f(node.as_ref()))
    }

    #[allow(clippy::redundant_closure)]
    fn map_children<F: FnMut(Self) -> DaftResult<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> DaftResult<Transformed<Self>> {
        let children = self.children();
        if !children.is_empty() {
            let new_children = children
                .into_iter()
                // for some reason rewrite_arc(plan, f) doesn't compile, but rewrite_arc(plan, |plan| f(plan)) does, so we use the latter
                .map_until_stop_and_collect(|plan| rewrite_arc(plan, |plan| f(plan)))?;
            if new_children.transformed {
                new_children
                    .map_data(|new_children| Ok(self.with_new_children(new_children.as_ref())))
            } else {
                Ok(Transformed::new(self, false, new_children.tnr))
            }
        } else {
            Ok(Transformed::no(self))
        }
    }
}

impl DynTreeNode for LogicalPlan {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        children: Vec<Arc<Self>>,
    ) -> DaftResult<Arc<Self>> {
        let old_children = arc_self.children();
        if children.len() != old_children.len() {
            panic!("LogicalPlan::with_new_arc_children: Wrong number of children")
        } else if children.is_empty()
            || children
                .iter()
                .zip(old_children.iter())
                .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
        {
            Ok(arc_self.with_new_children(&children).arced())
        } else {
            Ok(arc_self)
        }
    }
}

impl DynTreeNode for PhysicalPlan {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        children: Vec<Arc<Self>>,
    ) -> DaftResult<Arc<Self>> {
        let old_children = arc_self.children();
        if children.len() != old_children.len() {
            panic!("PhysicalPlan::with_new_arc_children: Wrong number of children")
        } else if children.is_empty()
            || children
                .iter()
                .zip(old_children.iter())
                .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
        {
            Ok(arc_self.with_new_children(&children).arced())
        } else {
            Ok(arc_self)
        }
    }
}

/// Applies `f` to rewrite a `Arc<T>` without copying, if possible
fn rewrite_arc<T, F: FnMut(T) -> DaftResult<Transformed<T>>>(
    plan: Arc<T>,
    mut f: F,
) -> DaftResult<Transformed<Arc<T>>>
where
    T: Clone,
{
    f(unwrap_arc(plan))?.map_data(|new_plan| Ok(Arc::new(new_plan)))
}

/// Converts a `Arc<T>` without copying, if possible. Copies the plan
/// if there is a shared reference
pub fn unwrap_arc<T>(plan: Arc<T>) -> T
where
    T: Clone,
{
    Arc::try_unwrap(plan)
        // if None is returned, there is another reference to this
        // T, so we can not own it, and must clone instead
        .unwrap_or_else(|node| node.as_ref().clone())
}
