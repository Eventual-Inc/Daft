use crate::ops::*;

#[derive(Clone)]
pub enum LogicalPlan {
    Source(Source),
}
