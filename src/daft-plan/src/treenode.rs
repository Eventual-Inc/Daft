use std::sync::Arc;

use crate::{physical_plan::PhysicalPlan, LogicalPlan};
use common_error::DaftResult;
use common_treenode::DynTreeNode;

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
