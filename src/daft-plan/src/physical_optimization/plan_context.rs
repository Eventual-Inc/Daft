use common_error::DaftResult;
use common_treenode::{ConcreteTreeNode, DynTreeNode};

use crate::PhysicalPlanRef;
// This struct allows providing context or state to go along
// with visiting TreeNodes.
pub(super) struct PlanContext<T: Sized> {
    pub plan: PhysicalPlanRef,
    pub context: T,
    pub children: Vec<Self>,
}

impl<T> PlanContext<T> {
    pub fn new(plan: PhysicalPlanRef, context: T, children: Vec<Self>) -> Self {
        Self {
            plan,
            context,
            children,
        }
    }

    pub fn with_plan(self, new_plan: PhysicalPlanRef) -> Self {
        Self::new(new_plan, self.context, self.children)
    }

    pub fn with_context(self, new_context: T) -> Self {
        Self::new(self.plan, new_context, self.children)
    }
}

impl<T: Default> PlanContext<T> {
    pub fn new_default(plan: PhysicalPlanRef) -> Self {
        let children = plan
            .arc_children()
            .into_iter()
            .map(Self::new_default)
            .collect();
        Self::new(plan, Default::default(), children)
    }
}

impl<T: Clone> PlanContext<T> {
    // Clone the context to the children
    pub fn propagate(mut self) -> Self {
        for child in &mut self.children {
            child.context = self.context.clone();
        }
        self
    }
}

impl<T> ConcreteTreeNode for PlanContext<T> {
    fn children(&self) -> Vec<&Self> {
        self.children.iter().collect()
    }

    fn take_children(mut self) -> (Self, Vec<Self>) {
        let children = std::mem::take(&mut self.children);
        (self, children)
    }

    fn with_new_children(mut self, children: Vec<Self>) -> DaftResult<Self> {
        self.children = children;
        let child_plans: Vec<PhysicalPlanRef> =
            self.children.iter().map(|x| x.plan.clone()).collect();
        self.plan = self.plan.with_new_children(&child_plans).arced();
        Ok(self)
    }
}
