use std::sync::Arc;

use crate::{physical_plan::PhysicalPlan, LogicalPlan};
use common_error::DaftResult;
use common_treenode::{TreeNode, VisitRecursion};

impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, op: &mut F) -> DaftResult<common_treenode::VisitRecursion>
    where
        F: FnMut(&Self) -> DaftResult<common_treenode::VisitRecursion>,
    {
        for child in self.children().into_iter() {
            match op(child.as_ref())? {
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
        let old_children = self.children();
        let new_children = old_children
            .iter()
            .map(|c| transform(c.as_ref().clone()).map(Arc::new))
            .collect::<DaftResult<Vec<_>>>()?;

        if old_children
            .into_iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != c2)
        {
            // At least one child was transformed, so we need to create a new node.
            Ok(self.with_new_children(new_children.as_slice()))
        } else {
            // No children were transformed, so we can return the current node unchanged.
            Ok(self)
        }
    }
}

impl TreeNode for PhysicalPlan {
    fn apply_children<F>(&self, op: &mut F) -> DaftResult<common_treenode::VisitRecursion>
    where
        F: FnMut(&Self) -> DaftResult<common_treenode::VisitRecursion>,
    {
        for child in self.children().into_iter() {
            match op(child.as_ref())? {
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
        let old_children = self.children();
        let new_children = old_children
            .iter()
            .map(|c| transform(c.as_ref().clone()).map(Arc::new))
            .collect::<DaftResult<Vec<_>>>()?;

        if old_children
            .into_iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != c2)
        {
            // At least one child was transformed, so we need to create a new node.
            Ok(self.with_new_children(new_children.as_slice()))
        } else {
            // No children were transformed, so we can return the current node unchanged.
            Ok(self)
        }
    }
}
