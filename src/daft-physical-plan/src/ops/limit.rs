use common_error::{ensure, DaftResult};
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limit {
    // Upstream node.
    pub input: PhysicalPlanRef,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    pub eager: bool,
    pub num_partitions: usize,
}

impl Limit {
    pub(crate) fn try_new(
        input: PhysicalPlanRef,
        offset: Option<u64>,
        limit: Option<u64>,
        eager: bool,
        num_partitions: usize,
    ) -> DaftResult<Self> {
        ensure!(
            offset.is_some() || limit.is_some(),
            "Invalid Limit, offset and limit are both None."
        );
        Ok(Self {
            input,
            offset,
            limit,
            eager,
            num_partitions,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![match (self.offset, self.limit) {
            (Some(o), Some(l)) => format!("Limit: {}..{}", o, l),
            (Some(o), None) => format!("Limit: {}..", o),
            (None, Some(l)) => format!("Limit: {}", l),
            (None, None) => unreachable!("Invalid Limit, offset and limit are both None."),
        }];

        res.push(format!("Eager = {}", self.eager));
        res.push(format!("Num partitions = {}", self.num_partitions));
        res
    }
}

crate::impl_default_tree_display!(Limit);
